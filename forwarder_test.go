package main_test

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	forwarder "github.com/m-mizutani/aws-falcon-data-forwarder"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Config struct {
	srcS3Bucket string
	srcS3Prefix string
	srcS3Region string

	dstGCSBucket string
	dstGCSPrefix string
	dstGCSRegion string

	SqsURL    string
	SecretArn string
}

func loadConfig() Config {
	cwd := os.Getenv("PWD")
	var fp *os.File
	var err error

	for cwd != "/" {
		cfgPath := filepath.Join(cwd, "test.json")

		cwd, _ = filepath.Split(strings.TrimRight(cwd, string(filepath.Separator)))

		fp, err = os.Open(cfgPath)
		if err == nil {
			break
		}
	}

	if fp == nil {
		log.Fatal("test.json is not found")
	}

	rawData, err := ioutil.ReadAll(fp)
	if err != nil {
		panic(err)
	}

	cfg := Config{}
	err = json.Unmarshal(rawData, &cfg)
	return cfg
}

func TestBuildConfig(t *testing.T) {
	_, err := forwarder.BuildArgs()

	assert.NoError(t, err)
}

func TestHandler(t *testing.T) {
	args, err := forwarder.BuildArgs()
	require.NoError(t, err)

	err = forwarder.Handler(args)
	require.NoError(t, err)
}

func TestReceiver(t *testing.T) {
	cfg := loadConfig()
	dataKey := "data/test_data.gz"

	sampleMessage := `{
		"cid": "abcdefghijklmn0123456789",
		"timestamp": 1492726639137,
		"fileCount": 4,
		"totalSize": 349986220,
		"bucket": "` + cfg.srcS3Bucket + `",
		"pathPrefix": "` + cfg.srcS3Prefix + `",
		"files": [
		  {
			"path": "` + cfg.srcS3Prefix + dataKey + `",
			"size": 89118480,
			"checksum": "d0f566f37295e46f28c75f71ddce9422"
		  }
		]
	  }`

	// Push test message.
	ssn := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	queue := sqs.New(ssn)
	_, err := queue.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(0),
		MessageBody:  aws.String(sampleMessage),
		QueueUrl:     &cfg.SqsURL,
	})

	require.NoError(t, err)
	os.Setenv("SECRET_ARN", cfg.SecretArn)
	defer os.Unsetenv("SECRET_ARN")

	args, err := forwarder.BuildArgs()
	require.NoError(t, err)

	msgCount := 0
	msgHandler := func(msg *forwarder.FalconMessage) error {
		msgCount++
		assert.Equal(t, "abcdefghijklmn0123456789", msg.CID)
		assert.Equal(t, 1, len(msg.Files))
		assert.Equal(t, cfg.srcS3Prefix+dataKey, msg.Files[0].Path)
		return nil
	}

	err = forwarder.ReceiveMessages(cfg.SqsURL, args.FalconAwsKey, args.FalconAwsSecret, msgHandler)
	require.NoError(t, err)
	assert.Equal(t, 1, msgCount)
}
