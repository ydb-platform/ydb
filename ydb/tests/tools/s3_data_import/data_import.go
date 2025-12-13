package main

import (
	"bytes"
	"context"
"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	mrand "math/rand"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/logging"
)

type humanBytes float64

func (b humanBytes) String() string {
	const KB = 1 << 10
	const MB = 1 << 20
	const GB = 1 << 30
	v := float64(b)
	u := "B"
	switch {
	case v >= GB:
		v /= GB
		u = "GiB"
	case v >= MB:
		v /= MB
		u = "MiB"
	case v >= KB:
		v /= KB
		u = "KiB"
	}

	return fmt.Sprintf("%.2f %s", v, u)
}

func genReader(n int, seed int64) io.ReadSeeker {
	if n <= 0 {
		return bytes.NewReader(nil)
	}

	buf := make([]byte, n)
	src := mrand.New(mrand.NewSource(seed))
	for i := 0; i < n; i += 1 << 20 {
		end := i + (1 << 20)
		if end > n {
			end = n
		}

		tmp := make([]byte, end-i)
		_, _ = io.ReadFull(src, tmp)
		copy(buf[i:end], tmp)
	}

	return bytes.NewReader(buf)
}

func main() {
	var (
		bucket      = flag.String("bucket", "", "bucket")
		endpoint    = flag.String("endpoint", "https://storage.yandexcloud.net", "endpoint")
		region      = flag.String("region", "ru-central1", "region")
		prefix      = flag.String("prefix", "bench/", "prefix")
		objects     = flag.Int("objects", 10, "objects")
		objSizeMiB  = flag.Int("object-size-mib", 512, "object size MiB")
		totalGiB    = flag.Float64("total-gb", 0, "total GiB")
		partSizeMiB = flag.Int64("part-size-mib", 32, "part size MiB")
		concurrency = flag.Int("concurrency", 16, "concurrency")
		timeoutSec  = flag.Int("timeout-sec", 600, "timeout sec")
		debugHTTP   = flag.Bool("debug-http", false, "debug-http")
	)

	flag.Parse()
	if *bucket == "" {
		log.Fatal("missing -bucket")
	}

	if *partSizeMiB < 5 {
		*partSizeMiB = 5
	}

	if *objSizeMiB <= 0 {
		*objSizeMiB = 512
	}

	objBytes := int64(*objSizeMiB) * (1 << 20)
	var plannedTotalBytes int64
	var sizes []int64
	if *totalGiB > 0 {
		plannedTotalBytes = int64(*totalGiB * float64(1<<30))
		if plannedTotalBytes <= 0 {
			log.Fatal("total-gb too small")
		}

		full := plannedTotalBytes / objBytes
		rem := plannedTotalBytes % objBytes
		n := int(full)
		if rem > 0 {
			n++
		}

		if n < 1 {
			n = 1
		}

		*objects = n
		sizes = make([]int64, n)
		for i := 0; i < n; i++ {
			sizes[i] = objBytes
		}

		if rem > 0 {
			sizes[n-1] = rem
		}
	} else {
		if *objects < 1 {
			*objects = 1
		}

		sizes = make([]int64, *objects)
		for i := 0; i < *objects; i++ {
			sizes[i] = objBytes
		}

		plannedTotalBytes = int64(*objects) * objBytes
	}

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, _ string, _ ...interface{}) (aws.Endpoint, error) {
		if service == s3.ServiceID {
			return aws.Endpoint{URL: *endpoint, HostnameImmutable: true}, nil
		}

		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{},
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   512,
		IdleConnTimeout:       90 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	httpClient := &http.Client{Transport: tr}

	load := []func(*config.LoadOptions) error{
		config.WithRegion(*region),
		config.WithEndpointResolverWithOptions(resolver),
		config.WithHTTPClient(httpClient),
	}

	if *debugHTTP {
		load = append(load,
			config.WithClientLogMode(aws.LogRequest|aws.LogResponse|aws.LogRetries),
			config.WithLogger(logging.NewStandardLogger(os.Stdout)),
		)
	}

	cfg, err := config.LoadDefaultConfig(context.Background(), load...)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	cfg.Retryer = func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) { o.MaxAttempts = 8 })
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })

	sem := make(chan struct{}, *concurrency)
	start := time.Now()
	var confirmed atomic.Int64

	done := make(chan struct{})
	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				n := confirmed.Load()
				el := time.Since(start).Seconds()
				if el > 0.5 {
					p := 0.0
					if plannedTotalBytes > 0 {
						p = 100.0 * float64(n) / float64(plannedTotalBytes)
						if p > 100 {
							p = 100
						}
					}

					avgMiB := float64(n) / (1 << 20) / el
					avgGiB := float64(n) / (1 << 30) / el
					log.Printf("progress %s/%s (%.1f%%) avg=%.2f MiB/s (%.3f GiB/s)", humanBytes(n), humanBytes(plannedTotalBytes), p, avgMiB, avgGiB)
				}
			case <-done:
				return
			}
		}
	}()

	ctxBG := context.Background()
	now := time.Now().UTC().Format("20060102-150405")
	makeKey := func(i int) string {
		var suf [4]byte
		rand.Read(suf[:])
		return filepath.ToSlash(filepath.Join(*prefix, fmt.Sprintf("%s-%05d-%s.bin", now, i, hex.EncodeToString(suf[:]))))
	}

	for i := 0; i < len(sizes); i++ {
		size := sizes[i]
		key := makeKey(i)
		if size < 5*(1<<20) {
			ctx, cancel := context.WithTimeout(ctxBG, time.Duration(*timeoutSec)*time.Second)
			body := genReader(int(size), time.Now().UnixNano()+int64(i))
			_, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(*bucket), Key: aws.String(key), Body: body})
			cancel()
			if err != nil {
				close(done)
				log.Fatalf("put %s: %v", key, err)
			}

			confirmed.Add(size)
			continue
		}

		ctx, cancel := context.WithTimeout(ctxBG, time.Duration(*timeoutSec)*time.Second)
		cmu, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(*bucket), Key: aws.String(key)})
		cancel()
		if err != nil {
			close(done)
			log.Fatalf("create multipart %s: %v", key, err)
		}

		uploadID := *cmu.UploadId

		partSize := *partSizeMiB * (1 << 20)
		if partSize < 5*(1<<20) {
			partSize = 5 * (1 << 20)
		}

		partCount := int(size / partSize)
		if size%partSize != 0 {
			partCount++
		}

		results := make([]types.CompletedPart, partCount)
		var wg sync.WaitGroup
		errOnce := atomic.Bool{}

		for pn := 1; pn <= partCount; pn++ {
			begin := int64(pn-1) * partSize
			ps := partSize
			if last := begin + ps; last > size {
				ps = size - begin
			}

			wg.Add(1)
			sem <- struct{}{}
			go func(pn int, partLen int64) {
				defer wg.Done()
				defer func() { <-sem }()
				if errOnce.Load() {
					return
				}

				body := genReader(int(partLen), time.Now().UnixNano()+int64(i*100000+pn))
				ctx, cancel := context.WithTimeout(ctxBG, time.Duration(*timeoutSec)*time.Second)
				out, err := client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(*bucket),
					Key:        aws.String(key),
					PartNumber: aws.Int32(int32(pn)),
					UploadId:   aws.String(uploadID),
					Body:       body,
				})

				cancel()
				if err != nil {
					errOnce.Store(true)
					return
				}

				results[pn-1] = types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(int32(pn))}
				confirmed.Add(partLen)
			}(pn, ps)
		}

		wg.Wait()

		if errOnce.Load() {
			ctx, cancel := context.WithTimeout(ctxBG, 60*time.Second)
			_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(*bucket), Key: aws.String(key), UploadId: aws.String(uploadID)})
			cancel()
			close(done)
			log.Fatalf("upload %s failed", key)
		}

		ctx, cancel = context.WithTimeout(ctxBG, 60*time.Second)
		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(*bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: results,
			},
		})

		cancel()
		if err != nil {
			close(done)
			log.Fatalf("complete %s: %v", key, err)
		}
	}

	close(done)
	n := confirmed.Load()
	el := time.Since(start).Seconds()
	log.Printf("Done: %s/%s (%.1f%%) time=%.2fs avg=%.2f MiB/s (%.3f GiB/s)",
		humanBytes(n), humanBytes(plannedTotalBytes),
		math.Min(100, 100*float64(n)/float64(plannedTotalBytes)),
		el, float64(n)/(1<<20)/el, float64(n)/(1<<30)/el)
}	
