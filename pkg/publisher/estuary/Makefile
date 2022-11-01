ifeq ($(shell which oapi-codegen),)
$(info "No oapi-codegen. Install it with `go install github.com/deepmap/oapi-codegen`")
endif

client.go: openapi.json
	oapi-codegen -package $$(basename $(shell pwd)) -generate types,client $^ > $@
