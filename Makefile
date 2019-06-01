bench:
	go test -run xxx -bench BenchmarkURLTop100 -benchmem -blockprofile block.out -cpuprofile cpu.out -memprofile mem.out -trace=trace.out -outputdir _pprof/${OUTPUT_DIR}

memprofile:
	go tool pprof -http=:8090 _pprof/mem.out