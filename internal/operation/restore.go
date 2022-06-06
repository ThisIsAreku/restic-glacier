package operation

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"regexp"
	"restic-glacier/internal/shared"
	"sync"
	"time"
)

var restoreStringRegexp = regexp.MustCompile(`([^=]+)="([^"]+)"(?:, ([^=]+)="([^"]+)")*`)

const (
	objectRestoreStateNone    = 0
	objectRestoreStatePending = 1
	objectRestoreStateDone    = 2
)

func parseRestoreString(str string) map[string]string {
	var r = make(map[string]string)
	groups := restoreStringRegexp.FindAllStringSubmatch(str, -1)
	if len(groups) == 1 {
		g := groups[0]
		r[g[1]] = g[2]

		if g[3] != "" {
			r[g[3]] = g[4]
		}
	}

	return r
}

var Restore = &opRestore{}

type opRestore struct {
	wg sync.WaitGroup
}

func (o *opRestore) FilterStorageClasses() []string {
	return []string{s3.StorageClassGlacier}
}

func (o *opRestore) OperateOnObject(ctx context.Context, svc *s3.S3, bucket string, object s3.Object) {
	if o.isObjectRestored(ctx, svc, bucket, object) == objectRestoreStatePending {
		shared.Logger.Infow("Object restoration is already pending", "key", *object.Key)
		o.queueWaitRestore(ctx, svc, bucket, object)
		return
	}

	shared.Ratelimit.Take()
	_, err := svc.RestoreObjectWithContext(ctx, &s3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    object.Key,
		RestoreRequest: &s3.RestoreRequest{
			Tier: aws.String(s3.TierStandard),
		},
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "failed to upload object, %v\n", err)
		}
		os.Exit(1)
	}

	o.queueWaitRestore(ctx, svc, bucket, object)
}

func (o *opRestore) WaitForCompletion() {
	o.wg.Wait()
}

func (o *opRestore) queueWaitRestore(ctx context.Context, svc *s3.S3, bucket string, object s3.Object) {
	o.wg.Add(1)
	// wait for object in goroutine
	go func(ctx context.Context, svc *s3.S3, bucket string, object s3.Object) {
		for {
			if o.isObjectRestored(ctx, svc, bucket, object) != objectRestoreStatePending {
				break
			}

			select {
			case <-ctx.Done():
				break
			case <-time.After(30 * time.Second):
			}
		}
		shared.Logger.Infow("Object is restored", "key", *object.Key)
		o.wg.Done()
	}(ctx, svc, bucket, object)
}

func (o *opRestore) isObjectRestored(ctx context.Context, svc *s3.S3, bucket string, object s3.Object) int {
	shared.Ratelimit.Take()
	result, err := svc.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    object.Key,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == request.CanceledErrorCode {
			// If the SDK can determine the request or retry delay was canceled
			// by a context the CanceledErrorCode error code will be returned.
			fmt.Fprintf(os.Stderr, "upload canceled due to timeout, %v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "failed to upload object, %v\n", err)
		}
		os.Exit(1)
	}

	if result.Restore == nil {
		return objectRestoreStateNone
	}

	restoreString := *result.Restore
	kv := parseRestoreString(restoreString)
	shared.Logger.Debugw("Object restoration status", "key", *object.Key, "currentStorageClass", *object.StorageClass, "restore", restoreString)
	if kv["ongoing-request"] == "true" {
		return objectRestoreStatePending
	}

	return objectRestoreStateDone
}
