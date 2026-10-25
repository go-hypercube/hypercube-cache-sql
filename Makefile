.PHONY: test lint fmt

test:
	go test ./...

lint:
	golangci-lint run ./...

fmt:
	gofmt -w *.go
