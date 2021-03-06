package configure

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	log "logging"
	"strings"
)

type VideoTypeEunm uint8

const (
	Camera VideoTypeEunm = iota
	PCCamera
	DesktopShare
)

type UserTypeEunm uint8

const (
	Bidder UserTypeEunm = iota
	Holder
	Machine
)

type RequestTypeEunm uint8

const (
	ProjectStart RequestTypeEunm = iota
	ProjectStop
)

type SubStaticPush struct {
	Master_prefix string
	Sub_prefix    string
}

type StaticPushInfo struct {
	Master_prefix string
	Upstream      string
}

type StaticPullInfo struct {
	Type   string
	Source string
	App    string
	Stream string
}

type ServerInfo struct {
	Servername      string
	Exec_push       []string
	Exec_push_done  []string
	Report          []string
	Static_push     []StaticPushInfo
	Static_pull     []StaticPullInfo
	Sub_static_push []SubStaticPush
}

type EngineInfo struct {
	Ffmpeg string
}

type ServerCfg struct {
	StaticAddr bool   `json:"staticAddr"`
	Notifyurl  string `json:"notifyUrl"`
	//RtmpBase     string       `json:"rtmpBase"`
	Listen       int          `json:"listen"`
	Hls          string       `json:"hls"`
	Hlsport      int          `json:"hlsPort"`
	Httpflv      string       `json:"httpFLV"`
	Flvport      int          `json:"flvPort"`
	Httpoper     string       `json:"httpOper"`
	Operport     int          `json:"operPort"`
	Chunksize    int          `json:"chunkSize"`
	EngineEnable string       `json:"engineEnable"`
	Engine       EngineInfo   `json:"engine"`
	Servers      []ServerInfo `json:"servers"`
}

type Url struct {
	PushId     int           `json:"pushId"`
	UserType   UserTypeEunm  `json:"userType"`
	VideoType  VideoTypeEunm `json:"videoType"`
	RtmpBase   string        `json:"rtmpBase"`
	VideoName  string        `json:"videoName"`
	SavePath   string        `json:"savePath"`
	SaveUrl    string        `json:"saveUrl"`
	RequestUrl string        `json:"requestUrl"`
}

type Live struct {
	LiveId string `json:"liveId"`
	Urls   []Url  `json:"urls"`
}

type LivesCfg struct {
	Lives []Live `json:"lives"`
}

type Replay struct {
	Addr   string `json:"addr"`   //????????????
	Size   int64  `json:"size"`   //??????????????????
	Md5    string `json:"md5"`    //???????????????md5
	Start  string `json:"start"`  //??????????????????
	Finish string `json:"finish"` //??????????????????
}

type PushStreamUrl struct {
	PushId     int           `json:"pushId"`     //??????ID
	RtmpBase   string        `json:"base"`       //??????????????????
	UserType   UserTypeEunm  `json:"userType"`   //????????????
	VideoType  VideoTypeEunm `json:"videoType"`  //????????????
	LimitAudio bool          `json:"limitAudio"` //?????????????????????
	State      int           `json:"state"`      //??????(0/1)
	PushUrl    string        `json:"url"`        //?????????URL??????
	SavePath   string        `json:"savePath"`   //????????????????????????
	VideoName  string        `json:"videoName"`  //????????????
	SaveUrl    string        `json:"saveUrl"`    //???????????????Url???root
	RequestUrl string        `json:"requestUrl"` //?????????URL???????????????????????????
	Replays    []*Replay     `json:"replays"`    //??????

}

type LiveRoom struct {
	LiveRoomId string           `json:"liveRoomId"` //????????????ID
	ProjectId  int              `json:"projectId"`  //??????ID
	Urls       []*PushStreamUrl `json:"urls"`       //??????????????????
}

type LiveRooms struct {
	Rooms []*LiveRoom `json:"lives"`
}

//
type PushResponse struct {
	LiveRoomId string `json:"liveRoomId"`
	PushId     int    `json:"pushId"`
	PushUrl    string `json:"pushUrl"`
	UserType   int    `json:"userType"`
	ProjectId  int    `json:"projectId"`
}

type ReplayStreamUrl struct {
	PushId  int       `json:"pushId"`  //??????ID
	Replays []*Replay `json:"replays"` //??????

}

type ReplayRoom struct {
	LiveRoomId string             `json:"liveRoomId"` //????????????ID
	Urls       []*ReplayStreamUrl `json:"urls"`       //??????????????????
}

type ReplayRooms struct {
	Rooms []*ReplayRoom `json:"replays"`
}

type FileToMd5 struct {
	FilePath string `json:"filePath"` //????????????ID
	Size     int64  `json:"size"`     //????????????ID
	MD5      string `json:"Md5"`      //??????????????????
	Start    string `json:"start"`    //??????????????????
	Finish   string `json:"finish"`   //??????????????????
}

type Md5s struct {
	Files []*FileToMd5 `json:"files"`
}

var RtmpServercfg ServerCfg
var LiveRtmpcfg LivesCfg
var isStaticPushEnable bool
var isSubStaticPushEnable bool

func LoadConfig(configfilename string) error {

	log.Infof("starting load configure file(%s)......", configfilename)
	data, err := ioutil.ReadFile(configfilename)
	if err != nil {
		log.Errorf("ReadFile %s error:%v", configfilename, err)
		return err
	}

	log.Infof("loadconfig: \r\n%s", string(data))

	err = json.Unmarshal(data, &RtmpServercfg)
	if err != nil {
		log.Errorf("json.Unmarshal error:%v", err)
		return err
	}
	log.Infof("get config json data:%v", RtmpServercfg)

	if RtmpServercfg.Chunksize == 0 {
		RtmpServercfg.Chunksize = 4096
	}
	log.Warning("Chunk size:", RtmpServercfg.Chunksize)

	isStaticPushEnable = false
	isSubStaticPushEnable = false
	for _, serverItem := range RtmpServercfg.Servers {
		if serverItem.Static_push != nil && len(serverItem.Static_push) > 0 {
			isStaticPushEnable = true
		}
		if serverItem.Sub_static_push != nil && len(serverItem.Sub_static_push) > 0 {
			isSubStaticPushEnable = true
		}
	}

	return nil
}

func LoadRtmpConfig(configfilename string) error {

	//??????????????????
	log.Infof("---->>>> Start Load Rtmp Configure File(%s)......", configfilename)
	data, err := ioutil.ReadFile(configfilename)
	if err != nil {
		log.Errorf("LoadRtmpConfig ReadFile=%s error=%v", configfilename, err)
		return err
	}
	log.Infof("---->>>> Load Rtmp Configure Data: \r\n%s", string(data))

	//??????Json??????
	log.Infof("---->>>> Load Rtmp Configure Unmarshal")
	err = json.Unmarshal(data, &LiveRtmpcfg)
	if err != nil {
		log.Errorf("---->>>> Load Rtmp Configure Unmarshal error:%v", err)
		return err
	}
	log.Infof("---->>>> Load Rtmp Configure Json data:%v", LiveRtmpcfg)

	return nil
}

func GetReportList() []string {
	var reportlist []string

	for _, serverItem := range RtmpServercfg.Servers {
		reportlist = append(reportlist, serverItem.Report...)
	}

	return reportlist
}

func GetExecPush() []string {
	var execList []string

	for _, serverItem := range RtmpServercfg.Servers {
		for _, item := range serverItem.Exec_push {
			execList = append(execList, item)
		}
	}
	return execList
}

func GetExecPushDone() []string {
	var execList []string

	for _, serverItem := range RtmpServercfg.Servers {
		for _, item := range serverItem.Exec_push_done {
			execList = append(execList, item)
		}
	}
	return execList
}

func GetChunkSize() int {
	return RtmpServercfg.Chunksize
}

func IsHttpOperEnable() bool {
	httpOper := strings.ToLower(RtmpServercfg.Httpoper)
	//log.Warning("http operation", httpOper)
	if httpOper == "enable" {
		return true
	}
	return false
}

func IsHttpFlvEnable() bool {
	flv := strings.ToLower(RtmpServercfg.Httpflv)
	//log.Warning("http-flv", flv)
	if flv == "enable" {
		return true
	}
	return false
}

func IsHlsEnable() bool {
	hls := strings.ToLower(RtmpServercfg.Hls)
	//log.Warning("HLS", hls)
	if hls == "enable" {
		return true
	}

	return false
}

//func GetLimit() int {
//	return RtmpServercfg.Limit
//}

//func GetChannels() (channels []string) {

//	channels = nil
//	for _, channel := range RtmpServercfg.Channels {

//		channels = append(channels, channel)
//	}

//	return
//}

//func ExistWildCard() bool {

//	for _, v := range RtmpServercfg.Channels {

//		if v == "*" {
//			return true
//		}
//	}
//	return false
//}

//func GetBlacks() (blacks []string) {

//	blacks = nil
//	for _, black := range RtmpServercfg.Blacks {

//		blacks = append(blacks, black)
//	}

//	return
//}

//func GetWhites() (whites []string) {

//	whites = nil
//	for _, white := range RtmpServercfg.Whites {

//		whites = append(whites, white)
//	}

//	return
//}

func GetListenPort() int {
	return RtmpServercfg.Listen
}

func GetHlsPort() int {
	return RtmpServercfg.Hlsport
}

func GetHttpFlvPort() int {
	return RtmpServercfg.Flvport
}

func GetHttpOperPort() int {
	return RtmpServercfg.Operport
}
func GetFfmpeg() string {
	return RtmpServercfg.Engine.Ffmpeg
}

func GetEngineEnable() string {
	return RtmpServercfg.EngineEnable
}

func GetStaticPullList() (pullInfoList []StaticPullInfo, bRet bool) {
	pullInfoList = nil
	bRet = false

	for _, serverinfo := range RtmpServercfg.Servers {
		if serverinfo.Static_pull != nil && len(serverinfo.Static_pull) > 0 {
			bRet = true
			pullInfoList = append(pullInfoList, serverinfo.Static_pull[:]...)
		}
	}

	return
}

func GetStaticPushUrlList(rtmpurl string) (retArray []string, bRet bool) {
	if !isStaticPushEnable {
		return nil, false
	}

	retArray = nil
	bRet = false

	//log.Printf("rtmpurl=%s", rtmpurl)
	url := rtmpurl[7:]

	index := strings.Index(url, "/")
	if index <= 0 {
		return
	}
	url = url[index+1:]
	//log.Printf("GetStaticPushUrlList: url=%s", url)
	for _, serverinfo := range RtmpServercfg.Servers {
		//log.Printf("server info:%v", serverinfo)
		for _, staticpushItem := range serverinfo.Static_push {
			masterPrefix := staticpushItem.Master_prefix
			upstream := staticpushItem.Upstream
			//log.Printf("push item: masterprefix=%s, upstream=%s", masterPrefix, upstream)
			if strings.Contains(url, masterPrefix) {
				newUrl := ""
				index := strings.Index(url, "/")
				if index <= 0 {
					newUrl = url
				} else {
					newUrl = url[index+1:]
				}
				destUrl := fmt.Sprintf("%s/%s", upstream, newUrl)
				retArray = append(retArray, destUrl)
				bRet = true
			}
		}
	}

	//log.Printf("GetStaticPushUrlList:%v, %v", retArray, bRet)
	return
}

func GetSubStaticMasterPushUrl(rtmpurl string) (retUpstream string, bRet bool) {
	if !isSubStaticPushEnable {
		return "", false
	}

	retUpstream = ""
	bRet = false

	url := rtmpurl[7:]

	index := strings.Index(url, "/")
	if index <= 0 {
		return
	}
	url = url[index+1:]

	bFoundFlag := false
	foundMasterPrefix := ""
	for _, serverinfo := range RtmpServercfg.Servers {
		for _, substaticpushItem := range serverinfo.Sub_static_push {
			masterPrefix := substaticpushItem.Master_prefix
			subPrefix := substaticpushItem.Sub_prefix
			if strings.Contains(url, subPrefix) {
				foundMasterPrefix = masterPrefix
				bFoundFlag = true
				break
			}
		}

		if bFoundFlag {
			for _, staticpushItem := range serverinfo.Static_push {
				masterPrefix := staticpushItem.Master_prefix
				upstream := staticpushItem.Upstream
				if foundMasterPrefix == masterPrefix {
					newPrefix := ""
					index := strings.Index(masterPrefix, "/")
					if index <= 0 {
						newPrefix = masterPrefix
					} else {
						newPrefix = masterPrefix[index+1:]
					}
					retUpstream = fmt.Sprintf("%s/%s", upstream, newPrefix)
					bRet = true
					return
				}
			}
			break
		}
	}

	return
}
