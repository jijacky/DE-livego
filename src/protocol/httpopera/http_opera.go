package httpopera

import (
	"av"
	cmap "concurrent-map"
	"configure"
	"encoding/json"
	"errors"
	"fmt"
	_ "io/ioutil"
	log "logging"
	"net/http"
	"os"
	"path"
	"protocol/rtmp"
	"protocol/rtmp/rtmprelay"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
)

var (
	logFilePath = "./"
	logFileName = "livego.log"
)

type Operation struct {
	Method string `json:"method"`
	URL    string `json:"url"`
	Stop   bool   `json:"stop"`
}

type OperationChange struct {
	Method    string `json:"method"`
	SourceURL string `json:"source_url"`
	TargetURL string `json:"target_url"`
	Stop      bool   `json:"stop"`
}

type ClientInfo struct {
	url              string
	rtmpRemoteClient *rtmp.Client
	rtmpLocalClient  *rtmp.Client
}

type Server struct {
	handler       av.Handler
	session       map[string]*rtmprelay.RtmpRelay
	sessionFlv    map[string]*rtmprelay.FlvPull
	sessionMRelay cmap.ConcurrentMap
	mrelayMutex   sync.RWMutex
	rtmpAddr      string

	listenString string
	webGin       *gin.Engine
}

func NewServer(h av.Handler, rtmpAddr string, operaListen string) *Server {

	server := &Server{
		handler:       h,
		session:       make(map[string]*rtmprelay.RtmpRelay),
		sessionFlv:    make(map[string]*rtmprelay.FlvPull),
		sessionMRelay: cmap.New(),
		rtmpAddr:      rtmpAddr,
	}
	server.webGin = gin.Default()
	gin.SetMode(gin.ReleaseMode)
	server.webGin.Use(logerMiddleware())
	server.webGin.Use(Cors())

	server.startWeb(operaListen)

	return server

}

type ReportStat struct {
	serverList  []string
	isStart     bool
	localServer *Server
}

type MRelayStart struct {
	Instancename string
	Dsturl       string
	Srcurlset    []rtmprelay.SrcUrlItem
	Buffertime   int
}

type MRelayAdd struct {
	Instanceid int64
	Srcurlset  []rtmprelay.SrcUrlItem
	Buffertime int
}
type MRelayReponse struct {
	Retcode      int
	Instanceid   int64
	Instancename string
	Dscr         string
}

var reportStatObj *ReportStat

/****gin需要处理的固定信息****/

//解决跨域问题
func Cors() gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Headers", "Content-Type,AccessToken,X-CSRF-Token, Authorization, Token")
		c.Header("Access-Control-Allow-Methods", "PUT, DELETE, POST, GET, OPTIONS")
		c.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type")
		c.Header("Access-Control-Allow-Credentials", "true")

		//放行所有OPTIONS方法
		if method == "OPTIONS" {
			c.AbortWithStatus(http.StatusNoContent)
		}
		// 处理请求
		c.Next()
	}
}

func logerMiddleware() gin.HandlerFunc {
	// 日志文件
	fileName := path.Join(logFilePath, logFileName)

	// 写入文件
	src, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println("err", err)
	}

	// 实例化
	logger := logrus.New()
	//设置日志级别
	logger.SetLevel(logrus.DebugLevel)
	//设置输出
	logger.Out = src
	// 设置 rotatelogs
	logWriter, err := rotatelogs.New(
		// 分割后的文件名称
		fileName+".%Y%m%d.log",

		// 生成软链，指向最新日志文件
		rotatelogs.WithLinkName(fileName),

		// 设置最大保存时间(7天)
		rotatelogs.WithMaxAge(7*24*time.Hour),

		// 设置日志切割时间间隔(1天)
		rotatelogs.WithRotationTime(24*time.Hour),
	)

	writeMap := lfshook.WriterMap{
		logrus.InfoLevel:  logWriter,
		logrus.FatalLevel: logWriter,
		logrus.DebugLevel: logWriter,
		logrus.WarnLevel:  logWriter,
		logrus.ErrorLevel: logWriter,
		logrus.PanicLevel: logWriter,
	}

	logger.AddHook(lfshook.NewHook(writeMap, &logrus.JSONFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
	}))

	return func(c *gin.Context) {
		//开始时间
		startTime := time.Now()
		//处理请求
		c.Next()
		//结束时间
		endTime := time.Now()
		// 执行时间
		latencyTime := endTime.Sub(startTime)
		//请求方式
		reqMethod := c.Request.Method
		//请求路由
		reqUrl := c.Request.RequestURI
		//状态码
		statusCode := c.Writer.Status()
		//请求ip
		clientIP := c.ClientIP()

		// 日志格式
		logger.WithFields(logrus.Fields{
			"status_code":  statusCode,
			"latency_time": latencyTime,
			"client_ip":    clientIP,
			"req_method":   reqMethod,
			"req_uri":      reqUrl,
		}).Info()

	}
}

//启动web接口
func (s *Server) startWeb(operaListen string) {

	//路由
	s.webGin.GET("getPush", s.handleGetPush)
	s.webGin.GET("getReplay", s.handleGetReplay)
	s.webGin.GET("stopProject", s.handleStopProject)
	s.webGin.GET("getCurrentList", s.handleGetCurrentList)
	s.webGin.GET("setPushIdAudio", s.handleSetAudioFromPushId)
	s.webGin.Run(operaListen)
}

type Stream struct {
	Key             string `json:"key"`
	Url             string `json:"Url"`
	PeerIP          string `json:"PeerIP"`
	StreamId        uint32 `json:"StreamId"`
	VideoTotalBytes uint64 `json:123456`
	VideoSpeed      uint64 `json:123456`
	AudioTotalBytes uint64 `json:123456`
	AudioSpeed      uint64 `json:123456`
}

type Streams struct {
	PublisherNumber int64
	PlayerNumber    int64
	Publishers      []Stream `json:"publishers"`
	Players         []Stream `json:"players"`
}

/*
得到推流点

格式：
http://127.0.0.1:8090/getPush

参数：
projectId: 项目ID
userType: 用户类型

地址举例：
http://127.0.0.1:8070/getPush?&projectId=12&userType=0
*/
func (s *Server) handleGetPush(c *gin.Context) {

	newProject := false
	var err error

	//获得参数信息
	tmpProjectId := c.Query("projectId")
	projectId, errInt := strconv.Atoi(tmpProjectId)
	if errInt != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "projectId Param error, please check them",
		})
		return
	}
	tmpUserType := c.Query("userType")
	userType, err := strconv.Atoi(tmpUserType)
	if err != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "projectId Param error, please check them",
		})
		return
	}
	log.Infof("Server handleGetPush projectId=%d userType=%d", projectId, userType)

	//得到Rtmp流的管理对象
	rtmpStream := s.handler.(*rtmp.RtmpStream)
	if rtmpStream == nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get rtmp Stream information error",
		})
		return
	}

	//判断项目是否存在
	var errAlloc error
	var liveRoomId string
	ret := rtmpStream.CheckProjectExits(projectId)

	if ret {

		//项目存在，得到项目所对应的房间ID
		err, liveRoom := rtmpStream.GetLiveRoom(projectId)
		if err != nil {

			c.JSON(601, gin.H{
				"result":  601,
				"message": "Get Push Failed Not Found ProjectId",
			})
			return
		}
		liveRoomId = liveRoom.LiveRoomId

	} else {

		//判断是否所有直播间已经满了
		if rtmpStream.LiveRoomFull() {

			//直播间已经满了，没有空闲的直播间
			c.JSON(601, gin.H{
				"result":  601,
				"message": "Get Push Failed Live Room Full",
			})

			return
		}

		errAlloc, liveRoomId = rtmpStream.AllocLiveRoomId(projectId)
		if errAlloc != nil {

			//分配直播间失败
			c.JSON(601, gin.H{
				"result":  601,
				"message": "Get Push Failed Alloc Live Room Failed",
			})
			return
		}

		newProject = true
	}

	//根据用户类型判断是否还有推流点
	if rtmpStream.PushUserFull(liveRoomId, configure.UserTypeEunm(userType)) {

		//已经满了不能再进行分配了
		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get Push Failed Live Room User Full Or UserType Not Found",
		})
		return
	}

	//分配一个推流点
	err, pushId, pushBase := rtmpStream.GetPushId(liveRoomId, configure.UserTypeEunm(userType))
	if err != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get Push Failed GetPushId Failed",
		})
		return
	}
	pushIdString := strconv.Itoa(pushId)

	//给用户分配直播推流点
	listenString := strconv.Itoa(configure.RtmpServercfg.Listen)
	var pushurl string
	if !configure.RtmpServercfg.StaticAddr {

		//推流地址不固定
		date := time.Now().Format("20060102")
		pushurl = pushBase + ":" + listenString + "/live/" + liveRoomId + "/" + date + "/" + tmpProjectId + "/Camera_" + pushIdString
	} else {

		//推流地址固定
		pushurl = pushBase + ":" + listenString + "/live/" + liveRoomId + "/" + tmpProjectId + "/Camera_" + pushIdString
	}

	/*启动推流点*/

	//设置key值
	keyString := "push:" + liveRoomId + "/" + pushIdString + "/" + tmpProjectId
	log.Infof("Server handleGetPush Create Key String %s Push Url %s", keyString, pushurl)

	//启动成功，进行设置
	rtmpStream.SetStartState(projectId, liveRoomId, pushId, pushurl, userType)
	ret = rtmpStream.CheckProjectExits(projectId)
	if !ret {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Stop Project Failed Not Found ProjectId",
		})
		return
	}

	//判断是否是分配的新项目
	if newProject {

		err, newProjectLiveRoom := rtmpStream.GetLiveRoomFromRoomId(liveRoomId)
		if err == nil {

			for _, v := range newProjectLiveRoom.Urls {

				if v.VideoType == configure.Camera {

					tmp := pushBase + ":" + listenString + "/live/" + liveRoomId + "/" + tmpProjectId + "/Camera_" + strconv.Itoa(v.PushId)
					if pushurl != tmp {

						//打开推流地址
						rtmpStream.SetStartState(projectId, liveRoomId, v.PushId, tmp, int(v.UserType))
					}

					requestUrl := v.RequestUrl + "?&roomId=" + liveRoomId + "&pushUrl=" + tmp
					s.requestUrl(requestUrl, configure.ProjectStart)
				}
			}
		}
	}

	response := &configure.PushResponse{
		LiveRoomId: liveRoomId,
		PushId:     pushId,
		PushUrl:    pushurl,
		UserType:   userType,
		ProjectId:  projectId,
	}
	c.JSON(http.StatusOK, response)
}

func (s *Server) handleGetPushTest(c *gin.Context) {

	newProject := false
	var err error

	//获得参数信息
	tmpProjectId := c.Query("projectId")
	projectId, errInt := strconv.Atoi(tmpProjectId)
	if errInt != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "projectId Param error, please check them",
		})
		return
	}
	tmpUserType := c.Query("userType")
	userType, err := strconv.Atoi(tmpUserType)
	if err != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "projectId Param error, please check them",
		})
		return
	}
	log.Infof("Server handleGetPushTest projectId=%d userType=%d", projectId, userType)

	//得到Rtmp流的管理对象
	rtmpStream := s.handler.(*rtmp.RtmpStream)
	if rtmpStream == nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get rtmp Stream information error",
		})
		return
	}

	//判断项目是否存在
	var errAlloc error
	var liveRoomId string
	ret := rtmpStream.CheckProjectExits(projectId)

	if ret {

		//项目存在，得到项目所对应的房间ID
		err, liveRoom := rtmpStream.GetLiveRoom(projectId)
		if err != nil {

			c.JSON(601, gin.H{
				"result":  601,
				"message": "Get Push Failed Not Found ProjectId",
			})
			return
		}
		liveRoomId = liveRoom.LiveRoomId

	} else {

		//判断是否所有直播间已经满了
		if rtmpStream.LiveRoomFull() {

			//直播间已经满了，没有空闲的直播间
			c.JSON(601, gin.H{
				"result":  601,
				"message": "Get Push Failed Live Room Full",
			})
			return
		}

		errAlloc, liveRoomId = rtmpStream.AllocLiveRoomId(projectId)
		if errAlloc != nil {

			//分配直播间失败
			c.JSON(601, gin.H{
				"result":  601,
				"message": "Get Push Failed Alloc Live Room Failed",
			})
			return
		}

		newProject = true
	}

	//根据用户类型判断是否还有推流点
	if rtmpStream.PushUserFull(liveRoomId, configure.UserTypeEunm(userType)) {

		//已经满了不能再进行分配了
		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get Push Failed Live Room User Full Or UserType Not Found",
		})
		return
	}

	//分配一个推流点
	err, pushId, pushBase := rtmpStream.GetPushId(liveRoomId, configure.UserTypeEunm(userType))
	if err != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get Push Failed GetPushId Failed",
		})
		return
	}
	pushIdString := strconv.Itoa(pushId)

	//给用户分配直播推流点
	listenString := strconv.Itoa(configure.RtmpServercfg.Listen)
	var pushurl string
	if !configure.RtmpServercfg.StaticAddr {

		//推流地址不固定
		date := time.Now().Format("20060102")
		pushurl = pushBase + ":" + listenString + "/live/" + liveRoomId + "/" + date + "/" + tmpProjectId + "/Camera_" + pushIdString
		//pushurl = pushBase + ":" + listenString + "/live/" + date + "/" + tmpProjectId[0] + "/Camera_" + pushIdString
	} else {

		//推流地址固定
		pushurl = pushBase + ":" + listenString + "/" + liveRoomId + "/Camera_" + pushIdString
		//pushurl = pushBase + ":" + listenString + "/live/" + tmpProjectId[0] + "/Camera_" + pushIdString
	}

	/*启动推流点*/

	//设置key值
	keyString := "push:" + liveRoomId + "/" + pushIdString
	//keyString := "push:" + pushIdString + "/" + tmpProjectId[0]
	log.Infof("Server handleGetPush Create Key String %s Push Url %s", keyString, pushurl)

	//启动成功，进行设置
	rtmpStream.SetStartState(projectId, liveRoomId, pushId, pushurl, userType)
	ret = rtmpStream.CheckProjectExits(projectId)
	if !ret {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Stop Project Failed Not Found ProjectId",
		})
		return
	}

	//判断是否是分配的新项目
	if newProject {

		err, newProjectLiveRoom := rtmpStream.GetLiveRoomFromRoomId(liveRoomId)
		if err == nil {

			for _, v := range newProjectLiveRoom.Urls {

				if v.VideoType == configure.Camera {

					requestUrl := v.RequestUrl + "?&roomId=" + liveRoomId + "&pushUrl=" + strconv.Itoa(v.PushId)
					s.requestUrl(requestUrl, configure.ProjectStart)
				}
			}
		}
	}

	//返回结构
	response := &configure.PushResponse{
		LiveRoomId: liveRoomId,
		PushId:     pushId,
		PushUrl:    pushurl,
		UserType:   userType,
		ProjectId:  projectId,
	}
	c.JSON(http.StatusOK, response)
	log.Infof("Server handleGetPushTest Return %x", response)
}

//断开指定的发布者链接
func (s *Server) closeReaderConn(c *gin.Context, url string) {

	//得到Rtmp流的管理对象closeReaderConn
	rtmpStream := s.handler.(*rtmp.RtmpStream)
	if rtmpStream == nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get rtmp Stream information error",
		})
		return
	}

	//计算发布者相关信息
	for item := range rtmpStream.GetStreams().IterBuffered() {

		if s, ok := item.Val.(*rtmp.Stream); ok {
			if s.GetReader() != nil {
				switch s.GetReader().(type) {
				case *rtmp.VirReader:
					v := s.GetReader().(*rtmp.VirReader)
					if v.Info().URL == url {
						v.Close(errors.New("Force Close Publisher Conn"))
					}
				}
			}
		}
	}

}

//断开指定的观看者链接
func (s *Server) closeWriteConn(c *gin.Context, url string) {

	//得到Rtmp流的管理对象closeWriteConn
	rtmpStream := s.handler.(*rtmp.RtmpStream)
	if rtmpStream == nil {
		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get rtmp Stream information error",
		})
		return
	}

	//统计观看者相关信息
	for item := range rtmpStream.GetStreams().IterBuffered() {

		ws := item.Val.(*rtmp.Stream).GetWs()

		for s := range ws.IterBuffered() {
			if pw, ok := s.Val.(*rtmp.PackWriterCloser); ok {
				if pw.GetWriter() != nil {
					switch pw.GetWriter().(type) {
					case *rtmp.VirWriter:

						v := pw.GetWriter().(*rtmp.VirWriter)
						if v.Info().URL == url {
							v.Close(errors.New("Force Close Viewers Conn"))
						}
					}
				}
			}
		}
	}
}

/*
项目停止

格式：
http://127.0.0.1:8090/stopProject

参数：
oper：操作类型（start、stop）
app：app类型（live）
projectId: 项目ID
userType: 用户类型

地址举例：
http://127.0.0.1:8090/stopProject?&projectId=12
*/
func (s *Server) handleStopProject(c *gin.Context) {

	//获得参数信息
	tmpProjectId := c.Query("projectId")
	projectId, errInt := strconv.Atoi(tmpProjectId)
	if errInt != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "projectId Param error, please check them",
		})
		return
	}
	log.Infof("Server handleStopProject projectId=%d", projectId)

	//得到Rtmp流的管理对象
	rtmpStream := s.handler.(*rtmp.RtmpStream)
	if rtmpStream == nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get rtmp Stream information error",
		})
		return
	}

	//判断项目是否存在
	ret := rtmpStream.CheckProjectExits(projectId)
	if !ret {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Stop Project Failed Not Found ProjectId",
		})
		return
	}

	//得到项目对应的房间
	errRoom, liveRoom := rtmpStream.GetLiveRoom(projectId)
	if errRoom != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Stop Project Failed Not Found ProjectId",
		})
		return
	}

	//推送项目结束通知
	for _, v := range liveRoom.Urls {

		if v.VideoType == configure.Camera {

			requestUrl := v.RequestUrl + "?roomId=" + liveRoom.LiveRoomId + "&pushId=" + strconv.Itoa(v.PushId)
			s.requestUrl(requestUrl, configure.ProjectStop)
		}
	}

	//轮询所有推流点进行终止推流
	log.Infof("Server handleStopProject Stop LiveRoomId=%s ProjectId=%d", liveRoom.LiveRoomId, liveRoom.ProjectId)
	for _, v := range liveRoom.Urls {

		if v.State == 1 {

			//关闭指定连接的发布者和观看者
			s.closeReaderConn(c, v.PushUrl)
			s.closeWriteConn(c, v.PushUrl)

			//删除信息
			v.LimitAudio = false
			v.State = 0
			v.PushUrl = ""
		}
	}
	liveRoom.ProjectId = -1

	c.JSON(http.StatusOK, gin.H{
		"result":  0,
		"message": "Stop Project Success",
	})

	log.Infof("Server handleStopProject Success ProjectId=%d", projectId)
}

/*
得到当前列表

格式：
http://127.0.0.1:8090/getCurrentList?&projectId=12

地址举例：
http://127.0.0.1:8090/getCurrentList?&projectId=12
*/
func (s *Server) handleGetCurrentList(c *gin.Context) {

	//得到Rtmp流的管理对象
	rtmpStream := s.handler.(*rtmp.RtmpStream)
	if rtmpStream == nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get rtmp Stream information error",
		})
		return
	}

	//获得参数信息
	tmpProjectId := c.Query("projectId")
	if tmpProjectId == "" {

		err, liveRooms := rtmpStream.GetRtmpList()
		if err != nil {

			c.JSON(601, gin.H{
				"result":  601,
				"message": "Get Rtmp List Failed",
			})
			return
		}
		c.JSON(http.StatusOK, liveRooms)

		log.Infof("Server handleGetCurrentList Return %x", liveRooms)

	} else {

		projectId, errInt := strconv.Atoi(tmpProjectId)
		if errInt != nil {

			c.JSON(601, gin.H{
				"result":  601,
				"message": "projectId Param error, please check them",
			})
			return
		}
		log.Infof("Server handleStopProject projectId=%d", projectId)

		err, liveRooms := rtmpStream.GetSingleRtmpList(projectId)
		if err != nil {

			c.JSON(601, gin.H{
				"result":  601,
				"message": "Get Rtmp List Failed",
			})
			return
		}
		c.JSON(http.StatusOK, liveRooms)

		log.Infof("Server handleGetCurrentList Return %x", liveRooms)
	}
}

/*
得到项目回看列表

格式：
http://127.0.0.1:8090/getReplay?&projectId=12

地址举例：
http://127.0.0.1:8090/getReplay?&projectId=12
*/
func (s *Server) handleGetReplay(c *gin.Context) {

	//得到Rtmp流的管理对象
	rtmpStream := s.handler.(*rtmp.RtmpStream)
	if rtmpStream == nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get rtmp Stream information error",
		})
		return
	}

	//获得参数信息
	tmpProjectId := c.Query("projectId")
	projectId, errInt := strconv.Atoi(tmpProjectId)
	if errInt != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "projectId Param error, please check them",
		})
		return
	}
	log.Infof("Server handleStopProject projectId=%d", projectId)

	//这里需要轮训所有的推流点所对应的的目录中是否存在此项目编号
	replayRooms := rtmpStream.GetProjectReplayList(projectId)

	c.JSON(http.StatusOK, replayRooms)

	log.Infof("Server handleGetReplay Return %x", replayRooms)

}

/*
声音控制

格式：
http://127.0.0.1:8090/setPushIdAudio?&projectId=12&pushId=1&audio=0

地址举例：
http://127.0.0.1:8090/setPushIdAudio?&projectId=12&pushId=1&audio=0
*/
func (s *Server) handleSetAudioFromPushId(c *gin.Context) {

	//获得参数信息
	tmpProjectId := c.Query("projectId")
	projectId, errInt := strconv.Atoi(tmpProjectId)
	if errInt != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "projectId Param error, please check them",
		})
		return
	}

	tmpPushId := c.Query("pushId")
	pushId, errPushId := strconv.Atoi(tmpPushId)
	if errPushId != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "pushId Param error, please check them",
		})
		return
	}

	tmpAudio := c.Query("audio")
	audio, errAudio := strconv.Atoi(tmpAudio)
	if errAudio != nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "audio Param error, please check them",
		})
		return
	}
	log.Infof("Server handleSetAudioFromPushId projectId=%d pushId=%d audio=%d", projectId, pushId, audio)

	//得到Rtmp流的管理对象
	rtmpStream := s.handler.(*rtmp.RtmpStream)
	if rtmpStream == nil {

		c.JSON(601, gin.H{
			"result":  601,
			"message": "Get rtmp Stream information error",
		})
		return
	}

	//设置声音控制
	rtmpStream.SetLimitAudioFromPushId(projectId, pushId, audio)

	writeString := fmt.Sprintf("set Audio Success ProjectId:%d pushId=%d Audio=%d", projectId, pushId, audio)

	c.JSON(http.StatusOK, gin.H{
		"result":  http.StatusOK,
		"message": writeString,
	})

	log.Infof("Server handleGetCurrentList %s", writeString)
}

func (s *Server) requestUrl(Url string, requestType configure.RequestTypeEunm) bool {

	var requestString string
	switch requestType {

	case configure.ProjectStart:
		requestString = Url + "&status=1"

	case configure.ProjectStop:
		requestString = Url + "&status=1"
	}

	resp, err := http.Get(requestString)
	if err != nil {

		log.Error("requestUrl Failed %s", requestString)
		return false
	}

	defer resp.Body.Close()
	//	_, errResp := ioutil.ReadAll(resp.Body)
	if resp.StatusCode == 200 {

		log.Infof("requestUrl Success %s", requestString)
		return true
	}
	return false
}

func NewReportStat(serverlist []string, localserver *Server) *ReportStat {
	return &ReportStat{
		serverList:  serverlist,
		isStart:     false,
		localServer: localserver,
	}
}

func (self *ReportStat) httpsend(data []byte) error {

	return nil
}

func (self *ReportStat) onWork() {
	rtmpStream := self.localServer.handler.(*rtmp.RtmpStream)
	if rtmpStream == nil {
		return
	}
	for {
		if !self.isStart {
			break
		}

		if self.serverList == nil || len(self.serverList) == 0 {
			log.Warning("Report statics server list is null.")
			break
		}

		msgs := new(Streams)
		msgs.PublisherNumber = 0
		msgs.PlayerNumber = 0

		for item := range rtmpStream.GetStreams().IterBuffered() {
			if s, ok := item.Val.(*rtmp.Stream); ok {
				if s.GetReader() != nil {
					switch s.GetReader().(type) {
					case *rtmp.VirReader:
						v := s.GetReader().(*rtmp.VirReader)
						msg := Stream{item.Key, v.Info().URL, v.ReadBWInfo.PeerIP, v.ReadBWInfo.StreamId, v.ReadBWInfo.VideoDatainBytes, v.ReadBWInfo.VideoSpeedInBytesperMS,
							v.ReadBWInfo.AudioDatainBytes, v.ReadBWInfo.AudioSpeedInBytesperMS}
						msgs.Publishers = append(msgs.Publishers, msg)
						msgs.PublisherNumber++
					}
				}
			}
		}

		for item := range rtmpStream.GetStreams().IterBuffered() {
			ws := item.Val.(*rtmp.Stream).GetWs()
			for s := range ws.IterBuffered() {
				if pw, ok := s.Val.(*rtmp.PackWriterCloser); ok {
					if pw.GetWriter() != nil {
						switch pw.GetWriter().(type) {
						case *rtmp.VirWriter:
							v := pw.GetWriter().(*rtmp.VirWriter)
							msg := Stream{item.Key, v.Info().URL, v.WriteBWInfo.PeerIP, v.WriteBWInfo.StreamId, v.WriteBWInfo.VideoDatainBytes, v.WriteBWInfo.VideoSpeedInBytesperMS,
								v.WriteBWInfo.AudioDatainBytes, v.WriteBWInfo.AudioSpeedInBytesperMS}
							msgs.Players = append(msgs.Players, msg)
							msgs.PlayerNumber++
						}
					}
				}
			}
		}
		resp, _ := json.Marshal(msgs)

		//log.Info("report statics server list:", self.serverList)
		//log.Info("resp:", string(resp))

		self.httpsend(resp)
		time.Sleep(time.Second * 5)
	}
}

func (self *ReportStat) Start() error {
	if self.isStart {
		return errors.New("Report Statics has already started.")
	}

	self.isStart = true

	go self.onWork()
	return nil
}

func (self *ReportStat) Stop() {
	if !self.isStart {
		return
	}

	self.isStart = false
}
