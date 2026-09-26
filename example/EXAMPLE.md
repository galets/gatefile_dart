# Example

Reads the document, updates it, prints `updated` events.

## Install gatefile

A release must be installed from https://github.com/galets/gatefile
in order for the `gatefile` command to be available.

Check it:

```sh
gatefile --version
```

## Launch gatefile server

Run each command in its own terminal. All commands work as-is.

Terminal 1: start a scratch server on port 18765:

```sh
echo "hello" > /tmp/gatefile-example.txt
DOCUMENT_PATH=/tmp/gatefile-example.txt \
API_KEY=demo \
ADDR=127.0.0.1:18765 \
BASE_URL=/gatefile/file \
gatefile
```

Terminal 2: run the example:

```sh
cd gatefile_dart
dart pub get
GATEFILE_URL=http://127.0.0.1:18765/gatefile/file \
GATEFILE_API_KEY=demo \
dart run example/main.dart
```

Terminal 3: trigger an event while the example runs (POST a change
with the current ETag):

```sh
ETAG=$(curl -s -D - -H "Authorization: Bearer demo" \
  http://127.0.0.1:18765/gatefile/file -o /dev/null \
  | grep -i '^etag:' | tr -d '\r' | awk '{print $2}')
curl -X POST \
  -H "Authorization: Bearer demo" \
  -H "Content-Type: text/plain" \
  -H "If-Match: $ETAG" \
  --data-binary "external change" \
  http://127.0.0.1:18765/gatefile/file
```

The example prints `event: ...` for each change.

Expected output:

```text
read: hello
updated: hello hello @ ...
event: ...external change...
```
