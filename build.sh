#!/bin/sh
DIR=${PWD}
cd $(dirname "$0")  
mkdir ${PWD}/build/bin -p
export CGO_ENABLED=0
export GOBIN=${PWD}/build/bin
export GOPATH=${PWD}/build/
export GOOS=linux
go get -v -d 
go test
go build
#go install 

GIT_COMMIT=$(git rev-parse HEAD)

docker build -t bpaxio/go-test:latest --build-arg GIT_COMMIT=$GIT_COMMIT .
cd $DIR

