import 'dart:async';
import 'dart:convert';
import 'dart:io';

typedef Content = String;
typedef ETag = String;

// Thrown when local copy is stale.
final class Conflict implements Exception {
  const Conflict([this.message = 'ETag mismatch']);

  final String message;

  @override
  String toString() => 'Conflict: $message';
}

// Minimal transport. Lets tests inject a fake.
abstract interface class GatefileTransport {
  // Returns (body, etag).
  Future<({Content body, ETag etag})> fetchDoc();
  Future<ETag> storeDoc(Content content, ETag etag);
  Future<Stream<String>> subscribe();
  void close();
}

// dart:io transport.
//
// Endpoints (see gatefile/doc/DESIGN.md):
// - GET {baseUrl} -> body + ETag header
// - POST {baseUrl} + If-Match -> new ETag header
// - GET {baseUrl}?subscribe -> text/event-stream of "<etag>\n\n"
final class IoTransport implements GatefileTransport {
  IoTransport({required this.baseUrl, required this.apiKey, HttpClient? http});

  final Uri baseUrl;
  final String apiKey;
  final HttpClient _http = HttpClient();
  bool _closed = false;

  Map<String, String> get _auth => {'Authorization': 'Bearer $apiKey'};

  @override
  Future<({Content body, ETag etag})> fetchDoc() async {
    final req = await _http.getUrl(baseUrl);
    _auth.forEach(req.headers.set);

    final res = await req.close();
    final body = await res.transform(utf8.decoder).join();
    _checkAuth(res.statusCode);

    if (res.statusCode != HttpStatus.ok) {
      throw HttpException('GET failed: ${res.statusCode}', uri: baseUrl);
    }

    return (body: body, etag: res.headers.value('etag') ?? '');
  }

  @override
  Future<ETag> storeDoc(Content content, ETag etag) async {
    final req = await _http.postUrl(baseUrl);
    _auth.forEach(req.headers.set);
    req.headers.set('If-Match', etag);
    req.headers.contentType = ContentType.text;
    req.write(content);

    final res = await req.close();
    await res.drain();
    _checkAuth(res.statusCode);

    if (res.statusCode == HttpStatus.conflict) {
      throw Conflict('current etag: ${res.headers.value('etag') ?? ''}');
    }

    if (res.statusCode == HttpStatus.badRequest) {
      throw StateError('If-Match header required');
    }

    if (res.statusCode != HttpStatus.ok) {
      throw HttpException('POST failed: ${res.statusCode}', uri: baseUrl);
    }

    return res.headers.value('etag') ?? '';
  }

  @override
  Future<Stream<String>> subscribe() async {
    final url = baseUrl.replace(
      queryParameters: {...baseUrl.queryParameters, 'subscribe': ''},
    );
    final req = await _http.getUrl(url);
    _auth.forEach(req.headers.set);
    req.headers.set('Accept', 'text/event-stream');

    final res = await req.close();
    _checkAuth(res.statusCode);

    if (res.statusCode != HttpStatus.ok) {
      await res.drain();
      throw HttpException('SSE failed: ${res.statusCode}', uri: url);
    }

    // Server sends "<etag>\n\n" per event. Split into etag tokens.
    return res
        .transform(utf8.decoder)
        .transform(const LineSplitter())
        .where((line) => line.trim().isNotEmpty)
        .map((line) => line.trim());
  }

  void _checkAuth(int code) {
    if (code == HttpStatus.unauthorized) {
      throw const AuthFailed();
    }
  }

  @override
  void close() {
    if (!_closed) {
      _closed = true;
      _http.close(force: true);
    }
  }
}

final class AuthFailed implements Exception {
  const AuthFailed();
  @override
  String toString() => 'AuthFailed: invalid API key';
}

// Single-document sync handle.
final class GatefileDocument {
  GatefileDocument({required Uri baseUrl, required String apiKey})
    : this.withTransport(
        transport: IoTransport(baseUrl: baseUrl, apiKey: apiKey),
      );

  GatefileDocument.withTransport({required GatefileTransport transport})
    : _transport = transport {
    _listenSse();
  }

  final GatefileTransport _transport;
  ETag _etag = '';
  bool _stale = false;
  bool _closed = false;
  final _updated = StreamController<void>.broadcast();

  // Current version. Empty before first get().
  ETag get etag => _etag;

  // Fetch body, store _etag, clear stale flag.
  Future<Content> get() async {
    _ensureOpen();
    final res = await _transport.fetchDoc();
    _etag = res.etag;
    _stale = false;
    return res.body;
  }

  // Full replace. Success updates _etag.
  // Throws Conflict if stale (SSE fired since last get/put,
  // or server reports 409).
  Future<void> put(Content content) async {
    _ensureOpen();

    if (_stale) {
      throw const Conflict();
    }

    try {
      _etag = await _transport.storeDoc(content, _etag);
    } on Conflict {
      _stale = true;
      rethrow;
    }

    _stale = false;
  }

  // Fires per server change. No payload. Call get() to refresh.
  Stream<void> get updated => _updated.stream;

  void close() {
    if (!_closed) {
      _closed = true;
      _updated.close();
      _transport.close();
    }
  }

  void _ensureOpen() {
    if (_closed) {
      throw StateError('GatefileDocument is closed');
    }
  }

  void _listenSse() async {
    try {
      final events = await _transport.subscribe();
      await for (final etag in events) {
        if (_closed) {
          break;
        }

        // SSE never updates _etag. Only get()/put() do.
        // Before first get() _etag is empty, so ignore all events.
        if (_etag.isEmpty) {
          continue;
        }

        if (etag != _etag) {
          _stale = true;
          if (!_updated.isClosed) {
            _updated.add(null);
          }
        }
      }
    } catch (_) {
      // SSE lossy by design. Ignore; next get() still works.
    }
  }
}
