package kodocli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"mime/multipart"
	"os"

	. "golang.org/x/net/context"
	"qiniupkg.com/x/rpc.v7"
)

const (
	typeDirect = "direct"
	typeCopy   = "copy"
)

type Part struct {
	// check fname first
	FileName string

	// then check R
	R io.Reader

	Crc32    uint32
	CheckCrc bool

	// finally, we use Key
	Key string

	// To == -1 means the end of file
	// [From, To)
	From, To int64
}

type part struct {
	Type string `json:"type"`

	Crc32 uint32 `json:"crc32"`

	StorageFile string `json:"storageFile"`
	Range       string `json:"range"`
}

type partArg struct {
	MimeType string `json:"mimeType"`
	Parts    []part `json:"parts"`
}

// for from base
func (p Uploader) PutParts(ctx Context, ret interface{}, uptoken, key string, hasKey bool, parts []Part, extra *PutExtra) error {

	if extra == nil {
		extra = &PutExtra{}
	}
	arg := partArg{MimeType: extra.MimeType}

	var b bytes.Buffer
	writer := multipart.NewWriter(&b)
	err := writeMultipartOfPart(writer, uptoken, key, hasKey, extra)

	for i, p := range parts {
		if p.FileName != "" {
			f, err := os.Open(p.FileName)
			if err != nil {
				return err
			}
			err = addDirectFile(writer, &arg, i, p.CheckCrc, p.Crc32, f)
			f.Close()
			if err != nil {
				return err
			}
		} else if p.R != nil {
			err = addDirectFile(writer, &arg, i, p.CheckCrc, p.Crc32, p.R)
			if err != nil {
				return err
			}
		} else {
			err := addCopyFile(&arg, i, p.Key, p.From, p.To)
			if err != nil {
				return err
			}
		}
	}

	argB, err := json.Marshal(arg)
	if err != nil {
		return err
	}
	err = writer.WriteField("parts", string(argB))
	if err != nil {
		return err
	}
	writer.Close()

	contentType := writer.FormDataContentType()
	return rpc.DefaultClient.CallWith64(ctx, ret, "POST", p.UpHosts[0]+"/parts", contentType, &b, int64(b.Len()))
}

func addDirectFile(writer *multipart.Writer, arg *partArg, idx int, checkCrc bool, crc32v uint32, r io.Reader) (err error) {

	fieldName := fmt.Sprintf("part-%d", idx)
	w, err := writer.CreateFormFile(fieldName, fieldName)
	if err != nil {
		return
	}

	if checkCrc && crc32v == 0 {
		rs, ok := r.(io.ReadSeeker)
		if !ok {
			err = errors.New("r should be io.ReadSeeker if generate crc32 in sdk")
			return
		}
		crch := crc32.NewIEEE()
		_, err = io.Copy(crch, rs)
		if err != nil {
			err = errors.New("io.Copy failed:" + err.Error())
			return
		}
		crc32v = crch.Sum32()
		_, err = rs.Seek(0, os.SEEK_SET)
		if err != nil {
			err = errors.New("rs.Seek failed:" + err.Error())
			return
		}
	}

	_, err = io.Copy(w, r)
	if err != nil {
		err = errors.New("io.Copy failed:" + err.Error())
		return
	}

	arg.Parts = append(arg.Parts, part{Type: typeDirect, Crc32: crc32v})
	return
}

func addCopyFile(arg *partArg, idx int, key string, from, to int64) (err error) {

	var rangeStr string
	if to == -1 || to > from {
		rangeStr = fmt.Sprintf("%d-%d", from, to)
	} else {
		return errors.New("invalid from&to argument")
	}

	arg.Parts = append(arg.Parts, part{Type: typeCopy, StorageFile: key, Range: rangeStr})
	return
}

func writeMultipartOfPart(writer *multipart.Writer, uptoken, key string, hasKey bool, extra *PutExtra) (err error) {

	// token
	if err = writer.WriteField("token", uptoken); err != nil {
		return
	}

	// key
	if hasKey {
		if err = writer.WriteField("key", key); err != nil {
			return
		}
	}

	// extra.Params
	if extra.Params != nil {
		for k, v := range extra.Params {
			err = writer.WriteField(k, v)
			if err != nil {
				return
			}
		}
	}
	return
}
