
all: build

version:
	@echo $(TAG)

proto: version
	@echo "build proto..."
	protoc *.proto --go_out=../../../

build: proto
	go test -cover ./...
	go build  -v  ./...

update:
	go get -u ./...

