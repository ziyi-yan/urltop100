test:
	go test -v -run Test
bench:
	go test -run xxx -bench BenchmarkURLTop100 -benchmem -blockprofile block.out -cpuprofile cpu.out -memprofile mem.out -trace=trace.out -outputdir _pprof/${OUTPUT_DIR}

memprofile:
	go tool pprof -http=:8090 _pprof/mem.out

build_docker:
	GOOS=linux go build .
	docker build . -f bench/Dockerfile -t urltop100

test_limit: build_docker
	docker run -v ${HOME}/misc/urltop100:/testdata \
				--memory=100m \
				-it urltop100 /bin/bash