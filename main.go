package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/sync/errgroup"
)

const (
	S3_REGION = "eu-west-1"
	S3_BUCKET = "glacial-io"
)

func main() {
	s, err := session.NewSession(&aws.Config{
		Region: aws.String(S3_REGION),
		Credentials: credentials.NewStaticCredentials(
			"XXX",
			"YYY",
			""),
	})

	if err != nil {
		log.Fatal(err)
	}

	err = uploadDirectoryToS3(s, ".")

	if err != nil {
		log.Fatal(err)
	}

}

func uploadDirectoryToS3(s *session.Session, dirname string) error {
	g, ctx := errgroup.WithContext(context.Background())
	var err1 error
	files, err := ioutil.ReadDir(dirname)
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		if !file.IsDir() {
			g.Go(uploadFileTos3(s, file.Name()))
		}
	}

	// Wait for all files uploading -> s3 to  complete.
	if err1 = g.Wait(); err1 == nil {
		fmt.Println("Successfully uploaded all files")
		return nil
	}

	return err1
}

func uploadFileTos3(s *session.Session, fileName string) error {

	// open the file for use
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	// get the file size and read
	// the file content into a buffer
	fileInfo, _ := file.Stat()
	var size = fileInfo.Size()
	buffer := make([]byte, size)
	file.Read(buffer)

	// config settings: this is where you choose the bucket,
	// filename, content-type and storage class of the file
	// you're uploading
	e, s3err := s3.New(s).PutObject(&s3.PutObjectInput{
		Bucket:               aws.String(S3_BUCKET),
		Key:                  aws.String(fileName),
		ACL:                  aws.String("private"),
		Body:                 bytes.NewReader(buffer),
		ContentLength:        aws.Int64(size),
		ContentType:          aws.String(http.DetectContentType(buffer)),
		ContentDisposition:   aws.String("attachment"),
		ServerSideEncryption: aws.String("AES256"),
		StorageClass:         aws.String("INTELLIGENT_TIERING"),
	})

	return s3err
}
