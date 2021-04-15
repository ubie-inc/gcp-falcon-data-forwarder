package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"

	"github.com/sirupsen/logrus"

	"cloud.google.com/go/storage"

	_ "github.com/GoogleCloudPlatform/berglas/pkg/auto"
)

const gcsWriteTimeout = 3600

var (
	logger = logrus.New()
)

func main() {
	port := os.Getenv("PORT")

	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)

	http.HandleFunc("/", handleRequest)
	http.ListenAndServe(":"+port, nil)
}

func handleRequest(w http.ResponseWriter, req *http.Request) {
	args, err := BuildArgs()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(http.StatusText(http.StatusInternalServerError)))
		return
	}

	err = Handler(args)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(http.StatusText(http.StatusInternalServerError)))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(http.StatusText(http.StatusOK)))
}

type awsCredential struct {
	key    string
	secret string
}

type S3Ptr struct {
	Region     string
	Bucket     string
	Key        string
	credential *awsCredential
}

type GCSPtr struct {
	Region string
	Bucket string
	Key    string
}

func Handler(args Args) error {
	forwardMessage := func(msg *FalconMessage) error {
		t := time.Unix(int64(msg.Timestamp/1000), 0)

		for _, f := range msg.Files {
			logger.WithField("f", f).Info("forwarding")

			src := S3Ptr{
				Region: args.FalconAwsRegion,
				Bucket: msg.Bucket,
				Key:    f.Path,
				credential: &awsCredential{
					key:    args.FalconAwsKey,
					secret: args.FalconAwsSecret,
				},
			}
			dst := GCSPtr{
				Region: args.GCSRegion,
				Bucket: args.GCSBucket,
				Key: strings.Join([]string{
					args.GCSPrefix,
					t.Format("2006/01/02/15/"),
					f.Path,
				}, ""),
			}

			err := ForwardS3File(src, dst)
			if err != nil {
				logger.Warn("Failed to forward: %s", err.Error())
				return err
			}
		}
		return nil
	}

	err := ReceiveMessages(args.SqsURL, args.FalconAwsKey, args.FalconAwsSecret,
		forwardMessage)
	return err
}

// BuildArgs builds argument of receiver from environment variables.
func BuildArgs() (Args, error) {
	return Args{
		GCSBucket:       os.Getenv("GCS_BUCKET"),
		GCSPrefix:       os.Getenv("GCS_PREFIX"),
		GCSRegion:       os.Getenv("GCS_REGION"),
		SqsURL:          os.Getenv("SQS_URL"),
		FalconAwsRegion: os.Getenv("FALCON_AWS_REGION"),
		FalconAwsKey:    os.Getenv("FALCON_AWS_KEY"),
		FalconAwsSecret: os.Getenv("FALCON_AWS_SECRET"), // Get value from Secret Manager via berglas
	}, nil
}

type Args struct {
	GCSBucket       string
	GCSPrefix       string
	GCSRegion       string
	SqsURL          string
	FalconAwsRegion string
	FalconAwsKey    string `json:"falcon_aws_key"`
	FalconAwsSecret string `json:"falcon_aws_secret"`
}

type FalconMessage struct {
	CID        string           `json:"cid"`
	Timestamp  uint             `json:"timestamp"`
	FileCount  int              `json:"fileCount"`
	TotalSize  int              `json:"totalSize"`
	Bucket     string           `json:"bucket"`
	PathPrefix string           `json:"pathPrefix"`
	Files      []FalconLogFiles `json:"files"`
}

type FalconLogFiles struct {
	Path     string `json:"path"`
	Size     int    `json:"size"`
	CheckSum string `json:"checksum"`
}

func sqsURLtoRegion(url string) (string, error) {
	urlPattern := []string{
		// https://sqs.ap-northeast-1.amazonaws.com/21xxxxxxxxxxx/test-queue
		`https://sqs\.([a-z0-9\-]+)\.amazonaws\.com`,

		// https://us-west-1.queue.amazonaws.com/2xxxxxxxxxx/test-queue
		`https://([a-z0-9\-]+)\.queue\.amazonaws\.com`,
	}

	for _, ptn := range urlPattern {
		re := regexp.MustCompile(ptn)
		group := re.FindSubmatch([]byte(url))
		if len(group) == 2 {
			return string(group[1]), nil
		}
	}

	return "", errors.New("unsupported SQS URL syntax")
}

// ReceiveMessages receives SQS message from Falcon side and invokes msgHandler per message.
// In this method, not use channel because SQS queue deletion must be after handling messages
// to keep idempotence.
func ReceiveMessages(sqsURL, awsKey, awsSecret string, msgHandler func(msg *FalconMessage) error) error {

	sqsRegion, err := sqsURLtoRegion(sqsURL)
	if err != nil {
		return err
	}

	cfg := aws.Config{Region: aws.String(sqsRegion)}
	if awsKey != "" && awsSecret != "" {
		cfg.Credentials = credentials.NewStaticCredentials(awsKey, awsSecret, "")
	} else {
		logger.Warn("AWS Key and secret are not set, use role permission")
	}

	queue := sqs.New(session.Must(session.NewSession(&cfg)))

	for {
		result, err := queue.ReceiveMessage(&sqs.ReceiveMessageInput{
			AttributeNames: []*string{
				aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			},
			MessageAttributeNames: []*string{
				aws.String(sqs.QueueAttributeNameAll),
			},
			QueueUrl:            &sqsURL,
			MaxNumberOfMessages: aws.Int64(1),
			VisibilityTimeout:   aws.Int64(36000), // 10 hours
			WaitTimeSeconds:     aws.Int64(0),
		})

		if err != nil {
			return errors.Wrap(err, "SQS recv error")
		}

		logger.WithField("result", result).Info("recv queue")

		if len(result.Messages) == 0 {
			break
		}

		for _, msg := range result.Messages {
			fmsg := FalconMessage{}
			err = json.Unmarshal([]byte(*msg.Body), &fmsg)
			if err != nil {
				return errors.Wrap(err, "Fail to parse Falcon SNS error")
			}

			if err = msgHandler(&fmsg); err != nil {
				return err
			}
		}

		_, err = queue.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      &sqsURL,
			ReceiptHandle: result.Messages[0].ReceiptHandle,
		})

		if err != nil {
			return errors.Wrap(err, "SQS queue delete error")
		}
	}

	return nil
}

func ForwardS3File(src S3Ptr, dst GCSPtr) error {
	cfg := aws.Config{Region: aws.String(src.Region)}
	if src.credential != nil {
		cfg.Credentials = credentials.NewStaticCredentials(src.credential.key,
			src.credential.secret, "")

	} else {
		logger.Warn("AWS Key and secret are not set, use role permission")
	}

	// Download
	downSrv := s3.New(session.Must(session.NewSession(&cfg)))
	getInput := &s3.GetObjectInput{
		Bucket: aws.String(src.Bucket),
		Key:    aws.String(src.Key),
	}

	getResult, err := downSrv.GetObject(getInput)
	if err != nil {
		return errors.Wrap(err, "Fail to download data from Falcon")
	}

	// Upload
	return writeGCS(dst.Bucket, dst.Key, getResult.Body)
}

func writeGCS(bucket, key string, data io.Reader) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	defer client.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*gcsWriteTimeout)
	defer cancel()
	writer := client.Bucket(bucket).Object(key).NewWriter(ctx)

	if _, err = io.Copy(writer, data); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("Writer.Close: %v", err)
	}

	return nil
}
