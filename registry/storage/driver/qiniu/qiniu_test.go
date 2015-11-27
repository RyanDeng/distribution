package qiniu

import (
	"os"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/testsuites"

	"gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { check.TestingT(t) }

func init() {
	accessKey := os.Getenv("QINIU_AK")
	secretKey := os.Getenv("QINIU_SK")

	adminAk := os.Getenv("ADMIN_AK")
	adminSk := os.Getenv("ADMIN_SK")

	driverConstructor := func() (storagedriver.StorageDriver, error) {
		parameters := DriverParameters{
			AccessKey: accessKey,
			SecretKey: secretKey,
			Bucket:    "registry-test",
			Domain:    "7xonbo.com0.z0.glb.clouddn.com",

			UserUid:         "1380448066",
			AdminAk:         adminAk,
			AdminSk:         adminSk,
			RefreshCacheUrl: "http://apihub-z0.qbox.me/mc/del",
		}

		return New(parameters)
	}

	skipCheck := func() string {
		if accessKey == "" {
			return "accessKey must be set to run Qiniu tests"
		}
		if secretKey == "" {
			return "secretKey must be set to run Qiniu tests"
		}
		return ""
	}

	testsuites.RegisterSuite(driverConstructor, skipCheck)
}
