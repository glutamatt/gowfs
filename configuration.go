package gowfs

import "fmt"
import "errors"
import "time"
import "net/url"
import "os/user"

const WebHdfsVer string = "/webhdfs/v1"

type Configuration struct {
	Addr                  string // host:port
	BasePath              string // initial base path to be appended
	User                  string // user.name to use to connect
	Password              string
	ConnectionTimeout     time.Duration
	DisableKeepAlives     bool
	DisableCompression    bool
	ResponseHeaderTimeout time.Duration
	MaxIdleConnsPerHost   int
	EnableHTTPS           bool
	EnableKnoxAuth        bool
}

func NewConfiguration() *Configuration {
	return &Configuration{
		ConnectionTimeout:     time.Second * 17,
		DisableKeepAlives:     false,
		DisableCompression:    true,
		ResponseHeaderTimeout: time.Second * 17,
	}
}

func (conf *Configuration) GetNameNodeUrl() (*url.URL, error) {
	if &conf.Addr == nil {
		return nil, errors.New("Configuration namenode address not set.")
	}

	scheme := "http"
	if conf.EnableHTTPS {
		scheme = "https"
	}
	var urlStr string = fmt.Sprintf("%s://%s%s%s", scheme, conf.Addr, WebHdfsVer, conf.BasePath)

	if &conf.User == nil || len(conf.User) == 0 {
		u, _ := user.Current()
		conf.User = u.Username
	}

	if conf.User != "" {
		urlStr = urlStr + "?user.name=" + conf.User
	}

	u, err := url.Parse(urlStr)

	if err != nil {
		return nil, err
	}

	return u, nil
}
