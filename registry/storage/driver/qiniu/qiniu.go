// Package qiniu provides a storagedriver.StorageDriver implementation to
// store blobs in Qiniu kodo blob storage.
//

package qiniu

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution/context"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"

	"qiniupkg.com/api.v7/kodo"
	"qiniupkg.com/api.v7/kodocli"
)

const driverName = "qiniu"

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKey string
	SecretKey string
	Bucket    string
	Domain    string
	//ChunkSize int64
}

func init() {
	factory.Register(driverName, &qiniuDriverFactory{})
}

type qiniuDriverFactory struct{}

func (factory *qiniuDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Domain string
	//Bucket string
	//ChunkSize int64

	KodoCli *kodo.Client
	Bucket  kodo.Bucket

	//pool  sync.Pool // pool []byte buffers used for WriteStream
	//zeros []byte // shared, zero-valued buffer used for WriteStream
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Qiniu
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

// FromParameters constructs a new Driver with a given parameters map
// Required parameters:
// - accesskey
// - secretkey
// - bucket
// - domain
func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	accessKey, ok := parameters["accesskey"]
	if !ok || fmt.Sprint(accessKey) == "" {
		return nil, fmt.Errorf("No accessKey parameter provided")
	}
	secretKey, ok := parameters["secretKey"]
	if !ok || fmt.Sprint(secretKey) == "" {
		return nil, fmt.Errorf("No secretKey parameter provided")
	}

	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket paramter provided")
	}

	domain, ok := parameters["domain"]
	if !ok || fmt.Sprint(domain) == "" {
		return nil, fmt.Errorf("No domain paramter provided")
	}

	params := DriverParameters{
		fmt.Sprint(accessKey),
		fmt.Sprint(secretKey),
		fmt.Sprint(bucket),
		fmt.Sprint(domain),
		//defaultChunkSize,
	}

	return New(params)
}

func New(params DriverParameters) (*Driver, error) {

	cli := kodo.New(0, &kodo.Config{
		AccessKey: params.AccessKey,
		SecretKey: params.SecretKey,
	})

	d := &driver{
		Domain: params.Domain,
		Bucket: cli.Bucket(params.Bucket),
		//ChunkSize: params.ChunkSize,
		//zeros:     make([]byte, params.ChunkSize),

		KodoCli: cli,
	}

	// d.pool.New = func() interface{} {
	// 	return make([]byte, d.ChunkSize)
	// }

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

func (d *driver) Name() string {
	return driverName
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {

	_, err := d.Stat(ctx, path)
	if err != nil {
		return nil, err
	}

	privateUrl := d.downloadUrl(path)
	resp, err := http.Get(privateUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return ioutil.ReadAll(resp.Body)
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {

	return d.Bucket.Put(ctx, nil, path, bytes.NewReader(contents), int64(len(contents)), nil)

}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) ReadStream(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

	stat, err := d.Stat(ctx, path)
	if err != nil {
		return nil, err
	}

	if offset >= stat.Size() {
		return ioutil.NopCloser(bytes.NewReader(nil)), nil
	}

	url := d.downloadUrl(path)

	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Range", "bytes="+strconv.FormatInt(offset, 10)+"-")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// WriteStream stores the contents of the provided io.Reader at a
// location designated by the given path. The driver will know it has
// received the full contents when the reader returns io.EOF. The number
// of successfully READ bytes will be returned, even if an error is
// returned. May be used to resume writing a stream by providing a nonzero
// offset. Offsets past the current size will write from the position
// beyond the end of the file.
func (d *driver) WriteStream(ctx context.Context, path string, offset int64, reader io.Reader) (totalRead int64, err error) {

	uptoken := d.KodoCli.MakeUptoken(&kodo.PutPolicy{
		Scope:    d.Bucket.Name + ":" + path,
		Expires:  3600,
		Accesses: []string{path},
	})

	uploader := kodocli.NewUploader(0, nil)

	writeWholeFile := false

	pathNotFoundErr := storagedriver.PathNotFoundError{Path: path}

	stat, err := d.Stat(ctx, path)
	if err != nil {
		if err.Error() == pathNotFoundErr.Error() {
			writeWholeFile = true
		} else {
			return 0, err
		}

	}

	//write reader to local temp file
	tmpF, err := ioutil.TempFile("/tmp", "qiniu_driver")
	if err != nil {
		return 0, err
	}

	defer os.Remove(tmpF.Name())
	defer tmpF.Close()

	written, err := io.Copy(tmpF, reader)
	if err != nil {
		return 0, err
	}
	tmpF.Sync()
	_, err = tmpF.Seek(0, os.SEEK_SET)
	if err != nil {
		return 0, err
	}

	//------------------------

	if writeWholeFile == false {
		parts := make([]kodocli.Part, 0)

		if offset == 0 {
			part_Reader := kodocli.Part{
				FileName: "",
				R:        tmpF,
			}
			parts = append(parts, part_Reader)

			if written < stat.Size() {
				part_OriginFile2 := kodocli.Part{
					Key:  path,
					From: written,
					To:   -1,
				}
				parts = append(parts, part_OriginFile2)
			}

		} else if offset == stat.Size() { //因为parts_api有闭区间写错了，故这里先特殊判断offset == stat.Size()
			part_OriginFile1 := kodocli.Part{
				Key:  path,
				From: 0,
				To:   -1,
			}
			parts = append(parts, part_OriginFile1)

			part_Reader := kodocli.Part{
				FileName: "",
				R:        tmpF,
			}
			parts = append(parts, part_Reader)
		} else if offset < stat.Size() {
			part_OriginFile1 := kodocli.Part{
				Key:  path,
				From: 0,
				To:   offset,
			}
			parts = append(parts, part_OriginFile1)

			appendSize := written + offset
			part_Reader := kodocli.Part{
				FileName: "",
				R:        tmpF,
			}
			parts = append(parts, part_Reader)

			if appendSize < stat.Size() {
				part_OriginFile2 := kodocli.Part{
					Key:  path,
					From: appendSize,
					To:   -1,
				}
				parts = append(parts, part_OriginFile2)
			}
		} else if offset > stat.Size() {
			part_OriginFile1 := kodocli.Part{
				Key:  path,
				From: 0,
				To:   -1,
			}
			parts = append(parts, part_OriginFile1)

			zeroBytes := make([]byte, offset-stat.Size())
			part_ZeroPart := kodocli.Part{
				R: bytes.NewReader(zeroBytes),
			}
			parts = append(parts, part_ZeroPart)

			part_Reader := kodocli.Part{
				R: tmpF,
			}
			parts = append(parts, part_Reader)
		}
		err = uploader.PutParts(nil, nil, uptoken, path, true, parts, nil)
		if err != nil {
			return 0, err
		}
	} else {
		err := d.Bucket.PutFile(ctx, nil, path, tmpF.Name(), nil)
		if err != nil {
			return 0, err
		}
	}

	return written, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {

	entry, err := d.Bucket.Stat(ctx, path)
	isDir := false

	if err != nil {
		if strings.Contains(err.Error(), "no such file") {
			subFiles, errList := d.List(ctx, path)
			if errList != nil {
				return nil, errList
			}
			if len(subFiles) == 0 {
				return nil, storagedriver.PathNotFoundError{Path: path}
			}
			isDir = true

		} else {
			return nil, err
		}
	}

	fieldsInfo := storagedriver.FileInfoFields{
		Path:  path,
		IsDir: isDir,
	}
	if isDir == false {
		fieldsInfo.ModTime = time.Unix(entry.PutTime/1e7, 0)
		fieldsInfo.Size = entry.Fsize
	}
	return storagedriver.FileInfoInternal{
		FileInfoFields: fieldsInfo,
	}, nil
}

// List returns a list of the objects that are direct descendants of the given path.
func (d *driver) List(ctx context.Context, path string) ([]string, error) {

	queryPath := ""
	if strings.LastIndex(path, "/") != len(path)-1 {
		queryPath = path + "/"
	} else {
		queryPath = path
	}

	limitPerQuery := 100
	marker := ""
	entries := make([]kodo.ListItem, 0)
	folders := make([]string, 0)
	for {
		tmpEntries, tmpCommonPrefix, markerOut, err := d.Bucket.List(ctx, queryPath, "/", marker, limitPerQuery)
		if err != nil && err != io.EOF {
			return nil, err
		}
		entries = append(entries, tmpEntries...)
		folders = append(folders, tmpCommonPrefix...)
		marker = markerOut
		if err == io.EOF {
			break
		}
	}

	entryNames := make([]string, 0)
	for _, e := range entries {
		entryNames = append(entryNames, e.Key)
	}
	for _, f := range folders {
		// floders will include "/" (like /f1/), so remove the last "/"
		if f == "/" {
			entryNames = append(entryNames, f)
			continue
		}

		if strings.LastIndex(f, "/") != len(f)-1 {
			entryNames = append(entryNames, f)
		} else {
			entryNames = append(entryNames, f[:len(f)-1])
		}

	}
	return entryNames, nil
}

// Move moves an object stored at sourcePath to destPath, removing the
// original object.
// Note: This may be no more efficient than a copy followed by a delete for
// many implementations.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	_, errSrc := d.Stat(ctx, sourcePath)
	if errSrc != nil {
		return errSrc
	}

	err := d.Bucket.Move(ctx, sourcePath, destPath)
	if err != nil && err.Error() == "file exists" {
		errDel := d.Delete(ctx, destPath)
		if errDel != nil {
			return errors.New("cannot overwrite existed dest path")
		}
		err = d.Bucket.Move(ctx, sourcePath, destPath)
	}
	return err
}

// Delete recursively deletes all objects stored at "path" and its subpaths.
func (d *driver) Delete(ctx context.Context, path string) error {

	return d.delete(ctx, path)

}

func (d *driver) delete(ctx context.Context, path string) error {

	stat, err := d.Stat(ctx, path)
	if err != nil {
		return err
	}

	if stat.IsDir() {
		files, err := d.List(ctx, path)
		if err != nil {
			return err
		}

		var errMsg string
		for _, f := range files {
			errF := d.delete(ctx, f)
			if errF != nil {
				errMsg += fmt.Sprintf("faile to delete %s *** ", f)
			}
		}

		if errMsg != "" {
			return errors.New(errMsg)
		} else {
			return nil
		}
	} else {
		//it's a 'file', so delete it directly
		return d.Bucket.Delete(ctx, path)
	}

}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	return d.downloadUrl(path), nil
}

func (d *driver) downloadUrl(path string) string {
	baseUrl := kodo.MakeBaseUrl(d.Domain, path)
	privateUrl := d.KodoCli.MakePrivateUrl(baseUrl, nil)

	return privateUrl
}

// // getbuf returns a buffer from the driver's pool with length d.ChunkSize.
// func (d *driver) getbuf() []byte {
// 	return d.pool.Get().([]byte)
// }

// func (d *driver) putbuf(p []byte) {
// 	copy(p, d.zeros)
// 	d.pool.Put(p)
// }
