REVISION=$(git rev-parse --abbrev-ref HEAD)-$(git describe --abbrev=7 --always --tags)-$(date +%Y%m%d-%H:%M:%S)
echo "Revision: ${REVISION}"

go build -o target/message-hub -ldflags "-X main.revision=${REVISION}" .