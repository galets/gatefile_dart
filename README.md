# Dart client for gatefile.

Single-document sync over REST + SSE. Full replace only.
Version is server `ETag`. `put` sends `_etag` as `If-Match`.

## Interface

```dart
typedef Content = String;
typedef ETag = String;

final class Conflict implements Exception;

final class GatefileDocument {
  ETag _etag = '';
  Future<Content> get();
  Future<void> put(Content content);
  Stream<void> get updated;
  void close();
}
```

## Behavior

* `get()` returns body, stores `_etag`.
* `put()` replaces doc. Success updates `_etag`.
  Stale `_etag` throws `Conflict`. Re-`get`, merge, retry.
* `updated` fires per server change. No payload.
  Call `get()` to refresh. Unsent `put` after fire throws `Conflict`.
* `close()` stops SSE polling.

## Examples

```dart
final doc = GatefileDocument(baseUrl: base, apiKey: key);

// read-modify-write
try {
  final cur = await doc.get();
  await doc.put('$cur hello');
} on Conflict {
  final fresh = await doc.get();
  await doc.put('$fresh hello');
}

// subscribe
doc.updated.listen((_) async {
  final cur = await doc.get();
  render(cur);
});
```
