# MessagePack parser plugin for Embulk

Parses files encoded in MessagePack.

## Versions

| embulk-parser-msgpack | Embulk |
| ---- | ---- |
| v0.4.0 | Embulk v0.10.41 or earlier |
| v0.5.0 | Embulk v0.9 - v0.11 (It will stop working after Embulk v1.0.0.) |
| v0.6.0 | Embulk v0.10.42 and later, including v0.11 |

## Overview

* **Plugin type**: parser
* **Guess supported**: yes

## Configuration

- **row_encoding**: type of a row. "array" or "map" (enum, default: map)
- **file_encoding**: if a file includes a big array, set "array". Otherwise, if a file includes sequence of rows, set "sequence" (enum, default: sequence)
- **columns**: description (schema, default: a single Json typed column)

## Example

seed.yml:

```yaml
in:
  # here can use any file input plugin type such as file, s3, gcs, etc.
  type: file
  path_prefix: /path/to/file/or/directory
  parser:
    type: msgpack
```

Command:

```
$ embulk gem install embulk-parser-msgpack
$ embulk guess -g msgpack seed.yml -o config.yml
$ embulk run config.yml
```

The guessed config.yml will include column settings:

```yaml
in:
  type: any file input plugin type
  parser:
    type: msgpack
    row_encoding: map
    file_encoding: sequence
    columns:
    - {index: 0, name: a, type: long}
    - {index: 1, name: b, type: string}
```

For Maintainers
----------------

### Release

Modify `version` in `build.gradle` at a detached commit, and then tag the commit with an annotation.

```
git checkout --detach master

(Edit: Remove "-SNAPSHOT" in "version" in build.gradle.)

git add build.gradle

git commit -m "Release vX.Y.Z"

git tag -a vX.Y.Z

(Edit: Write a tag annotation in the changelog format.)
```

See [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) for the changelog format. We adopt a part of it for Git's tag annotation like below.

```
## [X.Y.Z] - YYYY-MM-DD

### Added
- Added a feature.

### Changed
- Changed something.

### Fixed
- Fixed a bug.
```

Push the annotated tag, then. It triggers a release operation on GitHub Actions after approval.

```
git push -u origin vX.Y.Z
```
