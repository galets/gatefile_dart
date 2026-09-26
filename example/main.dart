import 'dart:async';
import 'dart:io';

import 'package:gatefile_dart/gatefile_dart.dart';

// Read, update, watch for events.
//
// Config via env (see EXAMPLE.md):
// - GATEFILE_URL, e.g. http://127.0.0.1:18765/gatefile/file
// - GATEFILE_API_KEY
Future<void> main() async {
  final base = Uri.parse(
    Platform.environment['GATEFILE_URL'] ??
        'http://127.0.0.1:18765/gatefile/file',
  );
  final key = Platform.environment['GATEFILE_API_KEY'] ?? 'demo';

  final doc = GatefileDocument(baseUrl: base, apiKey: key);

  // Watch for server changes.
  final sub = doc.updated.listen((_) async {
    final cur = await doc.get();
    print('event: $cur');
  });

  // Read.
  final cur = await doc.get();
  print('read: $cur');

  // Update (read-modify-write with retry).
  try {
    await doc.put('$cur hello @ ${DateTime.now().toUtc()}');
  } on Conflict {
    final fresh = await doc.get();
    await doc.put('$fresh hello @ ${DateTime.now().toUtc()}');
  }

  print('updated: ${await doc.get()}');

  // Let one external change through so events trigger.
  await Future.delayed(const Duration(seconds: 5));

  await sub.cancel();
  doc.close();
}
