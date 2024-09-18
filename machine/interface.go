package machine

import "github.com/go-sohunjug/utils/machine/os"

// 定义一个接口
type OsMachineInterface interface {
	GetMachine() os.Information
	GetBoardSerialNumber() (string, error)
	GetPlatformUUID() (string, error)
	GetCpuSerialNumber() (string, error)
}
