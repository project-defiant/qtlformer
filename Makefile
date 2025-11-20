SHELL := /bin/bash
VERSION ?=  $(shell grep -m 1 'version = ' tools/pyproject.toml | sed 's/version = "\(.*\)"/\1/')

build-images:
	docker build -t qtlformer:$(VERSION) .