package rtmp

import (
	"av"
	cmap "concurrent-map"
	"configure"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	log "logging"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"protocol/rtmp/cache"
	"protocol/rtmp/rtmprelay"
	"reflect"
	"strconv"
	"strings"
	_ "syscall"
	"time"
)

var (
	EmptyID = ""
)

type RtmpStream struct {
	streams   cmap.ConcurrentMap  //流管理（包括发布者和观看者）
	liveRooms configure.LiveRooms //直播房间管理
	md5s      configure.Md5s
}

func NewRtmpStream() *RtmpStream {

	ret := &RtmpStream{
		streams: cmap.New(),
	}

	ret.initLiveRooms()
	ret.loadReplayConfig()
	go ret.checkPublisher()
	go ret.checkMediaFile()

	return ret
}

func (rs *RtmpStream) loadReplayConfig() error {

	//读取配置文件
	log.Infof("---->>>> Start Load Replay Configure File")
	data, err := ioutil.ReadFile("replay.json")
	if err != nil {

		log.Errorf("loadReplayConfig error=%v", err)
		return err
	}
	log.Infof("---->>>> Load Replay Configure Data: \r\n%s", string(data))

	//读取Json配置
	log.Infof("---->>>> Load Replay Configure Unmarshal")
	err = json.Unmarshal(data, &rs.md5s)
	if err != nil {
		log.Errorf("---->>>> Load Replay Configure Unmarshal error:%v", err)
		return err
	}
	log.Infof("---->>>> Load Replay Configure Json data:%v", rs.md5s)

	return nil
}

func (rs *RtmpStream) writeReplayConfig() error {

	//读取配置文件
	log.Infof("---->>>> Start Write Replay Configure File")

	data, err := json.Marshal(rs.md5s)
	if err != nil {
		log.Errorf("writeReplayConfig Marshal error=%v", err)
		return err
	}

	err = ioutil.WriteFile("replay.json", data, os.ModeAppend)
	if err != nil {

		log.Errorf("writeReplayConfig WriteFile error=%v", err)
		return err
	}

	log.Infof("---->>>> Write Replay Configure Data: \r\n%s", string(data))
	return nil
}

func (rs *RtmpStream) pathExists(path string) (bool, error) {

	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (rs *RtmpStream) getFilePath(url string) (error, string) {

	//获取此pushID所对应文件保存地址
	err, pushUrl, liveRoomId, projectId := rs.GetPushFromUrl(url)
	if err != nil {
		return err, ""
	}

	//得到数据目录
	basePath := pushUrl.SavePath

	//拼接目录
	outPath := fmt.Sprintf("%s/%s/%d", basePath, liveRoomId, projectId)

	//判断文件是否存在
	exist, err := rs.pathExists(outPath)
	if err != nil {

		log.Errorf("Check Path Failed! [%v]\n", err)
		return errors.New("Check Path Failed"), ""
	}

	//目录不存在则创建目录
	if !exist {

		// 创建文件夹
		err := os.MkdirAll(outPath, os.ModePerm)
		if err != nil {

			log.Errorf("MkAll Path Failed [%v]\n", err)
			return errors.New("MkAll Path Failed"), ""
		}
	}

	//创建FFMpeg保存的文件名称
	timeNow := time.Now()
	timeString := timeNow.Format("20060102T150405")
	outFile := fmt.Sprintf("%s/%s_%s.ts", outPath, pushUrl.VideoName, timeString)

	for {

		fileExists, _ := rs.pathExists(outFile)
		if !fileExists {

			return nil, outFile
		} else {

			time.Sleep(time.Duration(1) * time.Second)
			outFile = fmt.Sprintf("%s/%s_%s.ts", outPath, pushUrl.VideoName, time.Now().Format("20060102T150405"))
		}
	}
	return errors.New("Get FFMpeg File Failed"), ""
}

func (rs *RtmpStream) StreamExist(key string) bool {

	log.Infof("RtmpStream StreamExist %s", key)

	if i, ok := rs.streams.Get(key); ok {

		if ns, stream_ok := i.(*Stream); stream_ok {

			log.Errorf("RtmpStream Stream Exist %s", ns.info.String())
			return true
		}
	}

	return false
}

//rtmp://10.10.60.62:1935/live/01/12/Camera_1
func (rs *RtmpStream) parserUrl(Url string) (string, int, string, string) {

	seplit := "/"
	paramArray := strings.Split(Url, seplit)

	flag := false
	postion := 0
	var liveRoomId string
	var projectId int
	var date string
	var cameraType string
	for _, v := range paramArray {

		if v == "live" {
			flag = true
			continue
		}

		if flag {

			if postion == 0 {

				liveRoomId = v
				postion++
			} else if postion == 1 {

				projectId, _ = strconv.Atoi(v)
				postion++
			} else if postion == 2 {

				comma := strings.Index(v, "_")

				cameraType = v[0:comma]
			}
		}
	}
	return liveRoomId, projectId, date, cameraType
}

//发布者
func (rs *RtmpStream) HandleReader(r av.ReadCloser) {

	info := r.Info()
	log.Infof("RtmpStream HandleReader %s", info.String())

	var stream *Stream
	i, ok := rs.streams.Get(info.Key)
	if stream, ok = i.(*Stream); ok {

		//发布者地址有流再次推送过来
		log.Infof("RtmpStream HandleReader TransStop Old Stream")
		stream.TransStop()
		id := stream.ID()

		if id != EmptyID && id != info.UID {

			log.Infof("RtmpStream HandleReader NewStream")
			ns := NewStream(rs)
			stream.Copy(ns)
			stream = ns
			rs.streams.Set(info.Key, ns)
		}

		//判断是否启动FFmpeg
		if stream.cmdExec != nil {
			log.Infof("RtmpStream HandleReader Repeat come into HandleReader repeat process = %p", stream.cmdExec.Process)
		}

		if "enable" == configure.GetEngineEnable() {

			log.Infof("RtmpStream HandleReader configure.GetEngineEnable() = enable Startffmpeg %s", info.URL)

			if err, currFile := rs.getFilePath(info.URL); err == nil {

				log.Infof("RtmpStream HandleReader Start FFMpeg URL=%s File=%s", info.URL, currFile)
				go stream.Startffmpeg(info.URL, currFile)
			}
			log.Error("RtmpStream HandleReader Start FFMpeg Failed URL=%s", info.URL)
		}

	} else {

		//创建发布者
		log.Infof("RtmpStream HandleReader NewStream Key=%s", info.Key)
		stream = NewStream(rs)
		rs.streams.Set(info.Key, stream)
		stream.info = info

		if stream.cmdExec != nil {
			log.Infof("RtmpStream HandleReader first come into HandleReader first process = %p ", stream.cmdExec.Process)
		}

		if "enable" == configure.GetEngineEnable() {

			//liveRoomId, projectId, date, cameraType := rs.parserUrl(info.URL)
			if err, currFile := rs.getFilePath(info.URL); err == nil {

				log.Infof("RtmpStream HandleReader Start FFMpeg URL=%s File=%s", info.URL, currFile)
				go stream.Startffmpeg(info.URL, currFile)
			}
			log.Error("RtmpStream HandleReader Start FFMpeg Failed URL=%s", info.URL)
		}
	}

	//根据Url地址得到pushId
	err, liveRoomId, pushId := rs.GetPushIdFromUrl(info.URL)
	if err != nil {
		return
	}
	stream.AddReader(r, liveRoomId, pushId)
}

//观看者
func (rs *RtmpStream) HandleWriter(w av.WriteCloser) {

	info := w.Info()
	log.Infof("RtmpStream HandleWriter info %s, type %v", info.String(), reflect.TypeOf(w))

	var s *Stream
	ok := rs.streams.Has(info.Key)
	if !ok {

		log.Infof("RtmpStream NewStream %s", info.Key)
		s = NewStream(rs)
		rs.streams.Set(info.Key, s)
		s.info = info
	} else {

		log.Infof("RtmpStream HandleWriter Get %s", info.Key)
		item, ok := rs.streams.Get(info.Key)
		if ok {
			s = item.(*Stream)
			s.AddWriter(w)
		}
	}
}

func (rs *RtmpStream) GetStreams() cmap.ConcurrentMap {

	return rs.streams
}

func (rs *RtmpStream) CheckProjectExits(projectId int) bool {

	log.Infof("RtmpStream CheckProjectExits projectId=%d", projectId)

	for _, v := range rs.liveRooms.Rooms {

		log.Infof("RtmpStream CheckProjectExits get projectId=%d", v.ProjectId)
		if v.ProjectId == projectId {
			return true
		}
	}

	return false
}

func (rs *RtmpStream) GetLiveRoom(projectId int) (error, *configure.LiveRoom) {

	log.Infof("RtmpStream GetLiveRoom projectId=%d", projectId)
	for _, v := range rs.liveRooms.Rooms {

		if v.ProjectId == projectId {
			return nil, v
		}
	}
	return errors.New("Not Found Live Room"), nil
}

func (rs *RtmpStream) GetLiveRoomFromRoomId(liveRoomId string) (error, *configure.LiveRoom) {

	log.Infof("RtmpStream GetLiveRoomFromRoomId liveRoomId=%s", liveRoomId)
	for _, v := range rs.liveRooms.Rooms {

		if v.LiveRoomId == liveRoomId {
			return nil, v
		}
	}
	return errors.New("Not Found Live Room"), nil
}

func (rs *RtmpStream) LiveRoomFull() bool {

	log.Infof("RtmpStream Check LiveRoomFull")

	for _, v := range rs.liveRooms.Rooms {

		if v.ProjectId == -1 {
			return false
		}
	}

	return true
}

func (rs *RtmpStream) AllocLiveRoomId(projectId int) (error, string) {

	log.Infof("RtmpStream AllocLiveRoomId=%d", projectId)

	for _, v := range rs.liveRooms.Rooms {

		if v.ProjectId == -1 {
			return nil, v.LiveRoomId
		}
	}

	return errors.New("Live Room Full"), ""
}

func (rs *RtmpStream) PushFull(liveRoomId string) bool {

	log.Infof("RtmpStream PushFull liveRoomId=%s", liveRoomId)

	for _, v := range rs.liveRooms.Rooms {

		if v.LiveRoomId == liveRoomId {

			for _, value := range v.Urls {

				if value.State == 0 {
					return false
				}

			}
		}
	}
	return true
}

func (rs *RtmpStream) PushUserFull(liveRoomId string, UserType configure.UserTypeEunm) bool {

	log.Infof("RtmpStream PushUserFull liveRoomId=%s UserType=%d", liveRoomId, UserType)

	for _, v := range rs.liveRooms.Rooms {

		if v.LiveRoomId == liveRoomId {

			for _, value := range v.Urls {

				if value.State == 0 && value.UserType == UserType {

					return false
				}

			}
		}
	}
	return true
}

func (rs *RtmpStream) GetPushId(liveRoomId string, UserType configure.UserTypeEunm) (error, int, string) {

	log.Infof("RtmpStream GetPushId liveRoomId=%s UserType=%d", liveRoomId, UserType)

	for _, v := range rs.liveRooms.Rooms {

		if v.LiveRoomId == liveRoomId {

			for _, value := range v.Urls {

				if value.State == 0 && value.UserType == UserType {

					return nil, value.PushId, value.RtmpBase
				}

			}
		}
	}
	return errors.New("Not Found PushId"), -1, ""
}

func (rs *RtmpStream) FindPushStream(pushUrl string) (error, *configure.PushStreamUrl) {

	log.Infof("RtmpStream FindPushStream pushUrl=%s", pushUrl)

	for _, v := range rs.liveRooms.Rooms {

		for _, value := range v.Urls {

			if value.PushUrl == pushUrl && 1 == value.State {

				return nil, value
			}
		}

	}
	return errors.New("Not Found PushUrl"), nil
}

func (rs *RtmpStream) SetLimitAudioFromPushId(projectId int, pushId int, limit int) error {

	log.Infof("RtmpStream SetLimitAudioFromPushId liveRoomId=%d pushId=%d limit=%d", projectId, pushId, limit)

	for _, v := range rs.liveRooms.Rooms {

		if v.ProjectId == projectId {

			for _, value := range v.Urls {

				if value.PushId == pushId && 1 == value.State {

					value.LimitAudio = !(limit != 0)
					return nil
				}
			}
		}
	}
	return errors.New("Not Found SetLimitAudio")
}

func (rs *RtmpStream) GetLimitAudioFromPushId(projectId int, pushId int) (error, bool) {

	for _, v := range rs.liveRooms.Rooms {

		if v.ProjectId == projectId {

			for _, value := range v.Urls {

				if value.PushId == pushId {

					return nil, value.LimitAudio
				}
			}
		}
	}
	return errors.New("Not Found PushId"), false
}

func (rs *RtmpStream) SetLimitAudioFromUrl(Url string, limit bool) error {

	log.Infof("RtmpStream SetLimitAudioFromUrl Url=%s limit=%d", Url, limit)

	for _, v := range rs.liveRooms.Rooms {

		for _, value := range v.Urls {

			if value.PushUrl == Url && 1 == value.State {

				value.LimitAudio = limit
				return nil
			}
		}
	}
	return errors.New("Not Found SetLimitAudio")
}

func (rs *RtmpStream) GetPushIdFromUrl(Url string) (error, string, int) {

	log.Infof("RtmpStream GetPushIdFromUrl Url=%s", Url)

	for _, v := range rs.liveRooms.Rooms {

		for _, value := range v.Urls {

			if value.PushUrl == Url {

				return nil, v.LiveRoomId, value.PushId
			}
		}
	}
	return errors.New("Not Found SetLimitAudio"), "", -1
}

func (rs *RtmpStream) GetProjectPushIdFromUrl(Url string) (error, int, int) {

	log.Infof("RtmpStream GetProjectPushIdFromUrl Url=%s", Url)

	for _, v := range rs.liveRooms.Rooms {

		for _, value := range v.Urls {

			if value.PushUrl == Url {

				return nil, v.ProjectId, value.PushId
			}
		}
	}
	return errors.New("Not Found SetLimitAudio"), -1, -1
}

func (rs *RtmpStream) GetPushFromUrl(Url string) (error, *configure.PushStreamUrl, string, int) {

	log.Infof("RtmpStream GetPushFromUrl Url=%s", Url)

	for _, v := range rs.liveRooms.Rooms {

		for _, value := range v.Urls {

			if value.PushUrl == Url {

				return nil, value, v.LiveRoomId, v.ProjectId
			}
		}
	}
	return errors.New("Not Found GetPushFromUrl"), nil, "", -1
}

func (rs *RtmpStream) getLive(liveId string) (error, configure.Live) {

	log.Infof("getLive liveId=%s", liveId)
	for _, v := range configure.LiveRtmpcfg.Lives {

		if v.LiveId == liveId {

			return nil, v
		}
	}
	return errors.New("Not Found Live"), configure.Live{}
}

func (rs *RtmpStream) getUrl(pushId int, live configure.Live) (error, configure.Url) {

	log.Infof("getUrl pushId=%s", pushId)
	for _, v := range live.Urls {

		if v.PushId == pushId {

			return nil, v
		}
	}
	return errors.New("Not Found Url"), configure.Url{}
}

//保存地址路径格式
//save+"/"+liveID+"/"+pushID+"/"+data+"/
func (rs *RtmpStream) getLocalFiles(liveId string, pushId int, date string) (error, []string) {

	log.Infof("getTimerFiles liveId=%s channalId=%s date=%s", liveId, pushId, date)

	//通过配置得到这个保存的目录
	var localPath string
	var urls []string

	errLive, live := rs.getLive(liveId)
	if errLive != nil {

		log.Error("getTimerFiles Failed getLive err != nil")
		return errLive, urls
	}

	errUrl, url := rs.getUrl(pushId, live)
	if errUrl != nil {

		log.Error("getTimerFiles Failed getUrl err != nil")
		return errUrl, urls
	}

	//得到保存地址
	pushIdString := strconv.Itoa(pushId)
	localPath = url.SavePath + "/" + liveId + "/" + pushIdString + "/" + date + "/"

	//得到指定目录下的所有文件
	files, err := ioutil.ReadDir(localPath)
	if err != nil {

		log.Error("getTimerFiles Failed ReadDir err=%s", err.Error())
		return errors.New("Read Dir Failed"), urls
	}

	// 获取文件，并输出它们的名字
	for _, file := range files {

		filePath := localPath + file.Name()
		urls = append(urls, filePath)
	}

	return nil, urls

}

//得到文件大小
func (rs *RtmpStream) getFileSize(path string) int64 {

	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

func (rs *RtmpStream) getFileMd5(filename string, md5str *string) error {

	f, err := os.Open(filename)
	if err != nil {

		errString := "Open File Failed err:" + err.Error()
		return errors.New(errString)
	}

	defer f.Close()

	md5hash := md5.New()
	if _, err := io.Copy(md5hash, f); err != nil {

		errString := "Copy Data Failed err:" + err.Error()
		return errors.New(errString)
	}

	md5hash.Sum(nil)
	*md5str = fmt.Sprintf("%x", md5hash.Sum(nil))
	return nil
}

func (rs *RtmpStream) findFileMD5(path string, size int64) (bool, string) {

	for _, v := range rs.md5s.Files {

		if (v.FilePath == path) && (v.Size == size) {

			return true, v.MD5
		}
	}
	return false, ""
}

//得到路径下所有文件
func (rs *RtmpStream) getDirFiles(pathDir string) (error, []string, []int64, []string, []string, []string) {

	log.Infof("getDirFiles path=%s", pathDir)

	//得到指定目录下的所有文件
	var dirFiles []string
	var dirFilesSize []int64
	var dirFilesMd5 []string
	var startTimers []string
	var finishTimers []string
	files, err := ioutil.ReadDir(pathDir)
	if err != nil {

		log.Error("getDirFiles Failed ReadDir err=%s", err.Error())
		return errors.New("Read Dir Failed"), dirFiles, dirFilesSize, dirFilesMd5, startTimers, finishTimers
	}

	// 获取文件，并输出它们的名字
	for _, file := range files {

		filePath := pathDir + "/" + file.Name()
		fileSize := rs.getFileSize(filePath)
		dirFilesSize = append(dirFilesSize, fileSize)

		//分解文件的起始和终止时间
		nameWithSuffix := path.Base(filePath)
		//得到后缀
		suffix := path.Ext(nameWithSuffix)
		//得到文件名不带后缀
		nameOnly := strings.TrimSuffix(nameWithSuffix, suffix)

		arr := strings.Split(nameOnly, "_")

		if len(arr) != 3 {

			log.Error("Replay File=%s Name Split Failed")
			continue

		}
		startTimer := arr[1]
		startTimers = append(startTimers, startTimer)

		finishTimer := arr[2]
		finishTimers = append(finishTimers, finishTimer)

		log.Infof("Replay File=%s Start Timer=%s Finish Timer=%s", filePath, startTimer, finishTimer)

		exits, md5str := rs.findFileMD5(filePath, fileSize)
		if exits {

			dirFilesMd5 = append(dirFilesMd5, md5str)

		} else {

			//得到文件的md5
			log.Infof("Begin Get %s File MD5", filePath)
			md5Err := rs.getFileMd5(filePath, &md5str)
			if md5Err != nil {
				md5str = ""
			}
			log.Infof("Get Md5 %s File Md5 %s", filePath, md5str)
			dirFilesMd5 = append(dirFilesMd5, md5str)

			//记录新的文件MD5
			fileMd5 := &configure.FileToMd5{
				FilePath: filePath,
				Size:     fileSize,
				MD5:      md5str,
				Start:    startTimer,
				Finish:   finishTimer,
			}
			rs.md5s.Files = append(rs.md5s.Files, fileMd5)
		}
		dirFiles = append(dirFiles, file.Name())

	}
	//会写文件
	rs.writeReplayConfig()
	return nil, dirFiles, dirFilesSize, dirFilesMd5, startTimers, finishTimers
}

func (rs *RtmpStream) initLiveRooms() {

	log.Infof("RtmpStream initLiveRooms")

	//设置静态数据
	for _, value := range configure.LiveRtmpcfg.Lives {

		var liveRoom *configure.LiveRoom = new(configure.LiveRoom)
		liveRoom.LiveRoomId = value.LiveId
		liveRoom.ProjectId = -1
		for _, v := range value.Urls {

			var pushUrl *configure.PushStreamUrl = new(configure.PushStreamUrl)
			pushUrl.PushId = v.PushId
			pushUrl.UserType = v.UserType
			pushUrl.VideoType = v.VideoType
			pushUrl.RtmpBase = v.RtmpBase
			pushUrl.SavePath = v.SavePath
			pushUrl.VideoName = v.VideoName
			pushUrl.SaveUrl = v.SaveUrl
			pushUrl.RequestUrl = v.RequestUrl
			pushUrl.LimitAudio = false

			liveRoom.Urls = append(liveRoom.Urls, pushUrl)
		}
		rs.liveRooms.Rooms = append(rs.liveRooms.Rooms, liveRoom)
	}
	log.Infof("Get Static Json:%v", rs.liveRooms.Rooms)
}

//设置rtmp的状态
func (rs *RtmpStream) SetStartState(projectId int, liveRoomId string, pushId int, url string, user_type int) (err error) {

	log.Infof("RtmpStream SetStartState")

	for _, value := range rs.liveRooms.Rooms {

		if value.LiveRoomId == liveRoomId {
			value.ProjectId = projectId

			for _, v := range value.Urls {

				if v.PushId == pushId {

					v.PushUrl = url
					v.UserType = configure.UserTypeEunm(user_type)
					v.State = 1
					return nil
				}
			}

		}

		if value.LiveRoomId == liveRoomId {

			value.ProjectId = projectId
			log.Infof("RtmpStream SetStartState set ProjectId=%d", value.ProjectId)
			for _, v := range value.Urls {

				if v.PushId == pushId {

					v.PushUrl = url
					v.UserType = configure.UserTypeEunm(user_type)
					v.State = 1
					return nil
				}
			}
		}
	}
	return errors.New("SetStartState Not Found LiveRoomid/PushId/Url")
}

//获得当前所有的直播房间结构
func (rs *RtmpStream) GetRtmpList() (error, configure.LiveRooms) {

	log.Infof("---->>>> RtmpStream CreateRtmpList")

	//得到当前推流列表
	currentLiveRooms := rs.liveRooms

	//设置回看文件
	for _, v := range currentLiveRooms.Rooms {

		for _, value := range v.Urls {

			checkDir := value.SavePath + "/" + v.LiveRoomId + "/" + strconv.Itoa(v.ProjectId)
			err, dirFiles, dirFilesSize, dirFilesMd5, dirFilesStart, dirFilesFinish := rs.getDirFiles(checkDir)
			if err != nil {

				return nil, currentLiveRooms
			}
			value.Replays = value.Replays[0:0]

			for index, w := range dirFiles {

				replay := &configure.Replay{
					Addr:   value.SaveUrl + "/" + v.LiveRoomId + "/" + strconv.Itoa(v.ProjectId) + "/" + w,
					Size:   dirFilesSize[index],
					Md5:    dirFilesMd5[index],
					Start:  dirFilesStart[index],
					Finish: dirFilesFinish[index],
				}
				value.Replays = append(value.Replays, replay)
			}

		}
	}

	return nil, currentLiveRooms
}

//获得当前指定的直播房间结构
func (rs *RtmpStream) GetSingleRtmpList(projectId int) (error, configure.LiveRooms) {

	log.Infof("---->>>> RtmpStream GetSingleRtmpList")

	//得到当前推流列表
	var currentLiveRooms configure.LiveRooms

	//设置回看文件
	for _, v := range rs.liveRooms.Rooms {

		if v.ProjectId == projectId {

			liveRoom := &configure.LiveRoom{

				LiveRoomId: v.LiveRoomId,
				ProjectId:  v.ProjectId,
			}
			currentLiveRooms.Rooms = append(currentLiveRooms.Rooms, liveRoom)

			for _, value := range v.Urls {

				pushStreamUrl := value
				liveRoom.Urls = append(liveRoom.Urls, pushStreamUrl)

				checkDir := value.SavePath + "/" + v.LiveRoomId + "/" + strconv.Itoa(v.ProjectId)
				err, dirFiles, dirFilesSize, dirFilesMd5, dirFilesStart, dirFilesFinish := rs.getDirFiles(checkDir)
				if err != nil {

					return nil, currentLiveRooms
				}

				for index, w := range dirFiles {

					replay := &configure.Replay{
						Addr:   value.SaveUrl + "/" + v.LiveRoomId + "/" + strconv.Itoa(v.ProjectId) + "/" + w,
						Size:   dirFilesSize[index],
						Md5:    dirFilesMd5[index],
						Start:  dirFilesStart[index],
						Finish: dirFilesFinish[index],
					}
					pushStreamUrl.Replays = append(pushStreamUrl.Replays, replay)
				}

			}
		}
	}

	return nil, currentLiveRooms
}

//获得当前指定的直播房间结构
func (rs *RtmpStream) GetProjectReplayList(projectId int) configure.ReplayRooms {

	log.Infof("---->>>> RtmpStream GetProjectReplayList")

	//得到当前推流列表
	var replayRooms configure.ReplayRooms

	//设置回看文件
	for _, v := range rs.liveRooms.Rooms {

		replayRoom := &configure.ReplayRoom{

			LiveRoomId: v.LiveRoomId,
		}

		hased := false

		log.Infof("pool liveRoomId=%s", replayRoom.LiveRoomId)
		for _, value := range v.Urls {

			checkDir := value.SavePath + "/" + v.LiveRoomId + "/" + strconv.Itoa(projectId)
			log.Infof("pool Get Replay File Begin UrlDir=%s", checkDir)

			err, dirFiles, dirFilesSize, dirFilesMd5, dirFilesStart, dirFilesFinish := rs.getDirFiles(checkDir)
			if err != nil {

				continue
			}
			log.Infof("pool Get Replay File End UrlDir=%s", checkDir)

			hased = true
			replayStream := &configure.ReplayStreamUrl{
				PushId: value.PushId,
			}
			replayRoom.Urls = append(replayRoom.Urls, replayStream)

			for index, w := range dirFiles {

				replay := &configure.Replay{
					Addr:   value.SaveUrl + "/" + v.LiveRoomId + "/" + strconv.Itoa(projectId) + "/" + w,
					Size:   dirFilesSize[index],
					Md5:    dirFilesMd5[index],
					Start:  dirFilesStart[index],
					Finish: dirFilesFinish[index],
				}
				replayStream.Replays = append(replayStream.Replays, replay)
			}

		}

		if hased {
			replayRooms.Rooms = append(replayRooms.Rooms, replayRoom)
		}
	}
	log.Infof("---->>>> RtmpStream GetProjectReplayList---->>>>")

	return replayRooms
}

func (rs *RtmpStream) checkViewers(Url string) {

	//统计观看者相关信息
	for item := range rs.GetStreams().IterBuffered() {
		ws := item.Val.(*Stream).GetWs()
		for s := range ws.IterBuffered() {
			if pw, ok := s.Val.(*PackWriterCloser); ok {
				if pw.GetWriter() != nil {
					switch pw.GetWriter().(type) {
					case *VirWriter:
						v := pw.GetWriter().(*VirWriter)

						log.Infof(">>>>Current Viewers Url=%s PeerIp=%s\n", v.Info().URL, v.WriteBWInfo.PeerIP)
					}
				}
			}
		}
	}

}

//定时查询发布者和观看者信息
func (rs *RtmpStream) checkPublisher() {

	for {

		time.Sleep(time.Duration(5) * time.Second)

		log.Infof("Server CheckStatic ---->>>>")

		//发布者统计
		for item := range rs.GetStreams().IterBuffered() {

			if s, ok := item.Val.(*Stream); ok {
				if s.GetReader() != nil {
					switch s.GetReader().(type) {
					case *VirReader:
						v := s.GetReader().(*VirReader)

						log.Infof("Current Publisher Url=%s PeerIp=%s\n", v.Info().URL, v.ReadBWInfo.PeerIP)

						rs.checkViewers(v.Info().URL)
					}
				}
			}
		}

		//保活检测
		log.Infof("Server CheckAlive ---->>>>")
		for item := range rs.streams.IterBuffered() {

			v := item.Val.(*Stream)
			log.Infof("RtmpStream checkAlive %s", v.info.String())

			if v.CheckAlive() == 0 {
				log.Infof("RtmpStream checkAlive remove %s", item.Key)

				//查看对应的切片机起来没，起来的话，关闭切片机
				if v.FFmpeg {
					v.FFmpeg = false
					v.Stopffmpeg()
				}

				rs.streams.Remove(item.Key)
			}
		}
	}
}

///定期清空不存在的文件
func (rs *RtmpStream) checkMediaFile() {

	for {

		time.Sleep(time.Duration(5) * time.Second)

		log.Infof("Server checkMediaFile ---->>>>")

		for i := 0; i < len(rs.md5s.Files); {

			filePath := rs.md5s.Files[i].FilePath
			exits, _ := rs.pathExists(filePath)
			if !exits {

				rs.md5s.Files = append(rs.md5s.Files[:i], rs.md5s.Files[i+1:]...)
				log.Infof("checkMediaFile Delete Not Found File=%s", filePath)
			} else {
				i++
			}
		}
		rs.writeReplayConfig()

	}
}

type Stream struct {
	isStart    bool
	FFmpeg     bool
	cache      *cache.Cache
	r          av.ReadCloser
	ws         cmap.ConcurrentMap
	info       av.Info
	cmdExec    *exec.Cmd
	liveRoomId string
	pushId     int
	limitAudio bool
	rtmpStream *RtmpStream
	saveFile   string
}

type PackWriterCloser struct {
	init bool
	w    av.WriteCloser
}

func (p *PackWriterCloser) GetWriter() av.WriteCloser {
	return p.w
}

func NewStream(rs *RtmpStream) *Stream {
	return &Stream{
		cache:      cache.NewCache(),
		ws:         cmap.New(),
		FFmpeg:     false,
		rtmpStream: rs,
	}
}

func (s *Stream) ID() string {
	if s.r != nil {
		return s.r.Info().UID
	}
	return EmptyID
}

func (s *Stream) GetReader() av.ReadCloser {
	return s.r
}

func (s *Stream) GetWs() cmap.ConcurrentMap {
	return s.ws
}

func (s *Stream) Copy(dst *Stream) {
	for item := range s.ws.IterBuffered() {
		v := item.Val.(*PackWriterCloser)
		s.ws.Remove(item.Key)
		v.w.CalcBaseTimestamp()
		dst.AddWriter(v.w)
	}
}

func (s *Stream) Startffmpeg(Url string, SaveFile string) {

	time.Sleep(time.Duration(3) * time.Second)

	log.Infof("Startffmpeg---->>>>")

	//停止原有的拉流FFMpeg进程
	input := Url
	cmd := ""
	cmd += "a="
	cmd += input
	cmd += ";b=`ps  -ef |grep $a|grep -v grep`;if [ \"x$b\" != \"x\" ];then  ps  -ef|grep $a|grep -v grep|cut -c 9-15 | xargs kill -9 ;fi"
	log.Infof("Stop Old FFMpeg Process When Start FFMpeg cmd---->>>>\n %s", cmd)

	//执行进程停止
	lsCmd := exec.Command("/bin/sh", "-c", cmd)
	lsCmd.Run()

	args := configure.GetFfmpeg() + " -v verbose -i " + Url + " -codec copy " + SaveFile + "\n"

	log.Infof("Startffmpeg %s", args)

	cmdExec := exec.Command("/bin/sh", "-c", args)

	err := cmdExec.Start()
	if err != nil {

		log.Error("Startffmpeg Failed")
		return
	}

	s.saveFile = SaveFile
	s.FFmpeg = true
	err = cmdExec.Wait()
}

/*
启动FFMpeg

参数：
Url: rtmp的地址
SaveFile: 保存的文件

FFmpeg指令拼接:

ffmpeg -v verbose -i rtmp://10.10.60.62:1935/live/01/12/Camera_2 -codec copy C:\1.ts

*/
//func (s *Stream) Startffmpeg(Url string, SaveFile string) {

//	time.Sleep(time.Duration(3) * time.Second)

//	//停止原有的拉流FFMpeg进程
//	input := Url
//	cmd := ""
//	cmd += "a="
//	cmd += input
//	cmd += ";b=`ps  -ef |grep $a|grep -v grep`;if [ \"x$b\" != \"x\" ];then  ps  -ef|grep $a|grep -v grep|cut -c 9-15 | xargs kill -9 ;fi"
//	//log.Infof("Stop Old FFMpeg Process When Start FFMpeg cmd---->>>>\n %s", cmd)

//	//执行进程停止
//	lsCmd := exec.Command("/bin/sh", "-c", cmd)
//	lsCmd.Run()
//	lsCmd.Run()

//	//设置输出
//	null, _ := os.Open(os.DevNull)
//	attr := &os.ProcAttr{
//		Dir:   "/opt/",
//		Files: []*os.File{null, null, null},
//		Sys: &syscall.SysProcAttr{
//			Credential: &syscall.Credential{
//				Uid: 0,
//				Gid: 0,
//			},
//			Setpgid: true,
//		},
//	}

//	args := " -v verbose -i " + Url + " -codec copy " + SaveFile + "\n"
//	var proc *os.Process
//	var err error
//	var tryCount int
//	proc = nil
//	err = nil
//	tryCount = 0

//	//启动FFMpeg
//	for {
//		proc, err = os.StartProcess(configure.GetFfmpeg(), strings.Fields(args), attr)
//		if nil == err {

//			log.Infof("Start FFMpeg Success")
//			break
//		} else {
//			tryCount++
//			time.Sleep(time.Second)
//			if 5 == tryCount {

//				log.Error("Failed to Start FFMpeg With 5 Errors")
//				return
//			}
//			log.Error("Start FFMpeg failed!")
//		}
//	}
//	null.Close()

//	//检测FFMpeg是否被正常启动
//	s.process = proc
//	err = syscall.Kill(s.process.Pid, 0)
//	if err == nil {

//		log.Infof("Start FFMpeg Success Pid=%d args=%s", s.process.Pid, args)

//		s.FFmpeg = true
//		s.process.Wait()

//	} else {
//		log.Error("Start FFMpeg Failed Check Process Is Deaded Pid=%d args=%s", s.process.Pid, args)
//	}
//}

func (s *Stream) Stopffmpeg() {

	defer func() {
		if r := recover(); r != nil {
			log.Error("rtmp Stopffmpeg  panic: ", r)
		}
	}()

	//终止FFMpeg
	if nil != s.cmdExec {

		log.Infof("kill s.process: ", s.cmdExec.Process)
		s.cmdExec.Process.Kill()
	}

	s.FFmpeg = false
	s.cmdExec = nil

	//修改文件名称为结束时间
	curPath, _ := filepath.Split(s.saveFile)

	//得到文件名带后缀
	nameWithSuffix := path.Base(s.saveFile)
	//得到后缀
	suffix := path.Ext(nameWithSuffix)
	//得到文件名不带后缀
	nameOnly := strings.TrimSuffix(nameWithSuffix, suffix)

	//添加结束时间
	newName := nameOnly + "_" + time.Now().Format("20060102T150405") + suffix

	//得到新的文件名
	newMediaFile := curPath + "/" + newName

	//重命名
	err := os.Rename(s.saveFile, newMediaFile)
	if err != nil {

		log.Error("Media File ReName Failed %s", s.saveFile)
		return
	}
	log.Infof("Media File ReName Success %s", newMediaFile)
}

func (s *Stream) AddReader(r av.ReadCloser, liveRoomId string, pushId int) {

	s.r = r
	s.liveRoomId = liveRoomId
	s.pushId = pushId
	log.Infof("Stream AddReader Info=%s liveRoomId=%s pushId=%d", s.info.String(), liveRoomId, pushId)
	go s.TransStart()
}

func (s *Stream) AddWriter(w av.WriteCloser) {

	info := w.Info()
	log.Infof("AddWriter:%v", s.info)
	pw := &PackWriterCloser{w: w}
	s.ws.Set(info.UID, pw)

	log.Infof("AddWriter ws Count:%v", s.ws.Count())
}

func (s *Stream) StartSubStaticPush() (ret bool) {
	ret = false
	_, masterPushObj := rtmprelay.GetStaticPushObjectbySubstream(s.info.URL)
	if masterPushObj != nil {
		err := masterPushObj.StartSubUrl(s.info.URL)
		if err == nil {
			ret = true
		}
	}
	return
}

func (s *Stream) StopSubStaticPush() {
	_, masterPushObj := rtmprelay.GetStaticPushObjectbySubstream(s.info.URL)
	if masterPushObj != nil {
		masterPushObj.StopSubUrl(s.info.URL)
	}
}

/*检测本application下是否配置static_push,
如果配置, 启动push远端的连接*/
func (s *Stream) StartStaticPush() (ret bool) {

	ret = false
	log.Infof("StartStaticPush: current url=%s", s.info.URL)
	pushurllist, err := rtmprelay.GetStaticPushList(s.info.URL)
	if err != nil || len(pushurllist) < 1 {
		log.Errorf("StartStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for _, pushurl := range pushurllist {
		//pushurl := pushurl + "/" + streamname
		log.Infof("StartStaticPush: static pushurl=%s", pushurl)

		staticpushObj := rtmprelay.GetAndCreateStaticPushObject(pushurl)
		if staticpushObj != nil {
			if err := staticpushObj.Start(); err != nil {
				log.Errorf("StartStaticPush: staticpushObj.Start %s error=%v", pushurl, err)
			} else {
				log.Infof("StartStaticPush: staticpushObj.Start %s ok", pushurl)
				ret = true
			}
		} else {
			log.Errorf("StartStaticPush GetStaticPushObject %s error", pushurl)
		}
	}

	return
}

func (s *Stream) StopStaticPush() {
	log.Infof("StopStaticPush: current url=%s", s.info.URL)
	pushurllist, err := rtmprelay.GetStaticPushList(s.info.URL)
	if err != nil || len(pushurllist) < 1 {
		log.Errorf("StopStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for _, pushurl := range pushurllist {
		//pushurl := pushurl + "/" + streamname
		log.Infof("StopStaticPush: static pushurl=%s", pushurl)

		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			staticpushObj.Stop()
			rtmprelay.ReleaseStaticPushObject(pushurl)
			log.Infof("StopStaticPush: staticpushObj.Stop %s ", pushurl)
		} else {
			log.Errorf("StopStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
}

func (s *Stream) IsSubSendStaticPush() bool {
	ret := false
	_, masterPushObj := rtmprelay.GetStaticPushObjectbySubstream(s.info.URL)
	if masterPushObj != nil {
		ret = true
	}
	//log.Printf("IsSubSendStaticPush: %s, ret=%v", s.info.URL, ret)
	return ret
}

func (s *Stream) SendSubStaticPush(packet av.Packet) {
	index, masterPushObj := rtmprelay.GetStaticPushObjectbySubstream(s.info.URL)
	if masterPushObj == nil {
		return
	}

	packet.StreamIndex = uint32(index + 1)

	masterPushObj.WriteAvPacket(&packet)
}

func (s *Stream) IsSendStaticPush() bool {
	pushurllist, err := rtmprelay.GetStaticPushList(s.info.URL)
	if err != nil || len(pushurllist) < 1 {
		//log.Printf("SendStaticPush: GetStaticPushList error=%v", err)
		return false
	}

	for _, pushurl := range pushurllist {
		//pushurl := pushurl + "/" + streamname
		//log.Printf("SendStaticPush: static pushurl=%s", pushurl)

		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			return true
			//staticpushObj.WriteAvPacket(&packet)
			//log.Printf("SendStaticPush: WriteAvPacket %s ", pushurl)
		} else {
			log.Errorf("SendStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
	return false
}

func (s *Stream) SendStaticPush(packet av.Packet) {
	pushurllist, err := rtmprelay.GetStaticPushList(s.info.URL)
	if err != nil || len(pushurllist) < 1 {
		return
	}

	for _, pushurl := range pushurllist {
		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			staticpushObj.WriteAvPacket(&packet)
		} else {
			log.Errorf("SendStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
}

func (s *Stream) TransStart() {
	s.isStart = true
	var p av.Packet

	log.Infof("TransStart:%v", s.info)

	//根据是否进行转推
	ret := s.StartStaticPush()
	if !ret {
		if s.IsSubSendStaticPush() {
			s.StartSubStaticPush()
		}
	}

	for {
		if !s.isStart {
			log.Info("Stream stop: call closeInter", s.info)
			s.closeInter()
			return
		}

		//从网络中读取视频数据
		for {
			err := s.r.Read(&p)
			if err != nil {
				log.Error("Stream Read error:", s.info, err)
				s.isStart = false
				s.closeInter()
				return
			}
			break
		}

		if s.IsSendStaticPush() {

			log.Info("---->>>>Stream IsSendStaticPush")
			s.SendStaticPush(p)
		} else if s.IsSubSendStaticPush() {

			log.Info("---->>>>Stream IsSubSendStaticPush")
			s.SendSubStaticPush(p)
		}

		s.cache.Write(p)

		if s.ws.IsEmpty() {
			continue
		}

		for item := range s.ws.IterBuffered() {
			v := item.Val.(*PackWriterCloser)
			if !v.init {
				//log.Infof("cache.send: %v", v.w.Info())
				if err := s.cache.Send(v.w); err != nil {
					log.Infof("[%s] send cache packet error: %v, remove", v.w.Info(), err)
					s.ws.Remove(item.Key)
					continue
				}
				v.init = true
			} else {
				new_packet := p
				//writeType := reflect.TypeOf(v.w)
				//log.Infof("w.Write: type=%v, %v", writeType, v.w.Info())
				if err := v.w.Write(&new_packet); err != nil {
					//log.Infof("[%s] write packet error: %v, remove", v.w.Info(), err)
					s.ws.Remove(item.Key)
				}
			}
		}

	}
}

func (s *Stream) TransStop() {

	log.Infof("TransStop: %s", s.info.Key)

	if s.isStart && s.r != nil {
		s.r.Close(errors.New("stop old"))
	}

	s.isStart = false
}

func (s *Stream) CheckAlive() (n int) {

	if s.r != nil && s.isStart {

		if s.r.Alive() {
			n++
		} else {
			log.Error("CheckAlive Read Failed Timeout %s", s.info.String())
			s.r.Close(errors.New("read timeout"))
		}
	}

	for item := range s.ws.IterBuffered() {

		v := item.Val.(*PackWriterCloser)
		if v.w != nil {

			if !v.w.Alive() && s.isStart {
				s.ws.Remove(item.Key)
				log.Error("CheckAlive Write Failed Write Timeout :", s.info.String())
				v.w.Close(errors.New("write timeout"))
				continue
			}
			n++
		}

	}
	return
}

func (s *Stream) ExecPushDone(key string) {
	execList := configure.GetExecPushDone()

	for _, execItem := range execList {
		cmdString := fmt.Sprintf("%s -k %s", execItem, key)
		go func(cmdString string) {
			log.Info("ExecPushDone:", cmdString)
			cmd := exec.Command("/bin/sh", "-c", cmdString)
			_, err := cmd.Output()
			if err != nil {
				log.Info("Excute error:", err)
			}
		}(cmdString)
	}
}
func (s *Stream) closeInter() {

	if s.r != nil {

		//停止发布者
		if s.IsSendStaticPush() {
			s.StopStaticPush()
		} else if s.IsSubSendStaticPush() {
			s.StopSubStaticPush()
		}

		//停止发布者所启动的ffmpeg
		log.Infof("Stream closeInter Check FFMpeg ")
		if s.FFmpeg {

			log.Infof("Stream Kill Publisher Start FFMpeg")

			s.FFmpeg = false
			s.Stopffmpeg()
		}

		log.Infof("Stream closeInter Close Publisher: [%s]", s.r.Info().String())
		s.rtmpStream.GetStreams().Remove(s.r.Info().Key)

	}
	s.ExecPushDone(s.r.Info().Key)

	//删除观看者
	for item := range s.ws.IterBuffered() {
		v := item.Val.(*PackWriterCloser)
		if v.w != nil {
			if v.w.Info().IsInterval() {
				v.w.Close(errors.New("closed"))
				s.ws.Remove(item.Key)

				log.Infof("Stream closeInter Close Viewers [%v] And Remove \n", v.w.Info().String())
			}
		}
	}
}
