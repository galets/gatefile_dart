import 'dart:async';

import 'package:test/test.dart';
import 'package:gatefile_dart/gatefile_dart.dart';

final class FakeTransport implements GatefileTransport {
  FakeTransport({String body = '', String etag = 'e0'})
    : _body = body,
      _etag = etag;

  String _body;
  String _etag;
  final _sse = StreamController<String>.broadcast();
  bool closed = false;

  void serverUpdate(String body, String etag) {
    _body = body;
    _etag = etag;
    _sse.add(etag);
  }

  @override
  Future<({Content body, ETag etag})> fetchDoc() async =>
      (body: _body, etag: _etag);

  @override
  Future<ETag> storeDoc(Content content, ETag etag) async {
    if (etag != _etag) {
      throw const Conflict();
    }
    _body = content;
    _etag = 'e${_etag.length}${content.length}';
    return _etag;
  }

  @override
  Future<Stream<String>> subscribe() async {
    // Emit current etag first, like the real server.
    // Replay on listen so the event is never lost.
    Stream<String> replay() async* {
      yield _etag;
      yield* _sse.stream;
    }
    return replay();
  }

  @override
  void close() {
    closed = true;
    _sse.close();
  }
}

void main() {
  test('get returns body and stores etag', () async {
    final doc = GatefileDocument.withTransport(
      transport: FakeTransport(body: 'hello', etag: 'e1'),
    );
    expect(await doc.get(), 'hello');
    expect(doc.etag, 'e1');
    doc.close();
  });

  test('put success updates etag', () async {
    final doc = GatefileDocument.withTransport(
      transport: FakeTransport(body: 'a', etag: 'e1'),
    );
    await doc.get();
    await doc.put('ab');
    expect(doc.etag, isNot('e1'));
    expect(await doc.get(), 'ab');
    doc.close();
  });

  test('put with stale etag throws Conflict', () async {
    final t = FakeTransport(body: 'a', etag: 'e1');
    final doc = GatefileDocument.withTransport(transport: t);
    await doc.get();
    t.serverUpdate('b', 'e2');
    await Future.delayed(const Duration(milliseconds: 20));
    expect(() => doc.put('c'), throwsA(isA<Conflict>()));
    doc.close();
  });

  test('read-modify-write retry after Conflict', () async {
    final t = FakeTransport(body: 'cur', etag: 'e1');
    final doc = GatefileDocument.withTransport(transport: t);
    final cur = await doc.get();
    t.serverUpdate('fresh', 'e2');
    await Future.delayed(const Duration(milliseconds: 20));
    try {
      await doc.put('$cur hello');
      fail('expected Conflict');
    } on Conflict {
      final fresh = await doc.get();
      await doc.put('$fresh hello');
    }
    expect(await doc.get(), 'fresh hello');
    doc.close();
  });

  test('updated fires per server change, no payload', () async {
    final t = FakeTransport(body: 'a', etag: 'e1');
    final doc = GatefileDocument.withTransport(transport: t);
    await doc.get();
    // Drain initial SSE event.
    await Future.delayed(const Duration(milliseconds: 20));
    final fired = doc.updated.first;
    t.serverUpdate('b', 'e2');
    await fired;
    expect(await doc.get(), 'b');
    doc.close();
  });

  test('SSE never updates etag', () async {
    final t = FakeTransport(body: 'a', etag: 'e1');
    final doc = GatefileDocument.withTransport(transport: t);
    // Initial SSE event arrives before any get().
    await Future.delayed(const Duration(milliseconds: 20));
    expect(doc.etag, isEmpty);
    doc.close();
  });

  test('close stops and blocks get/put', () async {
    final doc = GatefileDocument.withTransport(
      transport: FakeTransport(),
    );
    doc.close();
    expect(() => doc.get(), throwsStateError);
    expect(() => doc.put('x'), throwsStateError);
  });
}
