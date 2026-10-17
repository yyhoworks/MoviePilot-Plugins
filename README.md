# MoviePilot Plugins

MoviePilot plugins by Felix Yang.

## CloudStrm

CloudStrm monitors a CD2-mounted directory for mobile cloud disks without an official API, such as China Mobile cloud disk, and generates STRM files that point to OpenList files. It also supports subtitles and common sidecar files such as NFO, posters, backdrops, and JSON files, then refreshes configured Emby libraries after generation.

Features:

- Monitor created, modified, moved, and deleted files.
- Wait for files to become stable before processing them.
- Generate STRM files using the configured OpenList URL template.
- Copy subtitles and related NFO/image/JSON files.
- Optional cleanup of generated files after source deletion.
- Refresh one or more configured Emby servers.
- Keep the existing `/strm` command for manual generation, including category paths such as `/strm foreign-movies`.
- Run a full scan with the `CloudStrmHelper` command.

## CloudStrmButler

`CloudStrmButler` is a focused MoviePilot plugin for realtime directory monitoring, full or targeted STRM generation, subtitle and sidecar copying, deletion cleanup, and Emby refresh. Version `2.1.18` keeps the workflow lightweight with a concise status dashboard, simple full-scan options, compatible `/strm` targeting, progress and cancellation, failure details with retry, recent run summaries, and restart-safe background processing. Its plugin ID is `CloudStrmButler`.

## QBAutoClean

`QBAutoClean` is a silent qBittorrent cleanup plugin (`QB 自动清理` in the UI):
it periodically deletes completed tasks while keeping all data files, with
exemptions by tag or tracker domain, an optional dry-run mode, a per-run
deletion cap, and a `/qbclean` command for manual runs (`dry` to preview).
Its plugin ID is `QBAutoClean`.

## MoviePilot market source

Subscribe to the repository itself in MoviePilot's plugin market settings:

```text
https://github.com/yyhoworks/MoviePilot-Plugins
```

Do not use the raw `package.json` URL as the market source. MoviePilot reads
`package.v3.json` for v3, `package.v2.json` for v2, and the matching
`plugins.v3/` or `plugins.v2/` directory automatically.

For MoviePilot v3, add the repository to `PLUGIN_MARKET` together with any
existing sources, separated by commas, then refresh the plugin market:

```text
https://github.com/jxxghp/MoviePilot-Plugins,https://github.com/yyhoworks/MoviePilot-Plugins
```

The repository currently contains `CloudStrmHelper` (displayed as
`CloudStrm`), `CloudStrmButler` (displayed as `云盘Strm小管家`) and
`QBAutoClean` (displayed as `QB 自动清理`). Future plugins can be added to
this same repository by adding their metadata to the package index and
their code under the matching `plugins.v3/<plugin-id>/` and/or
`plugins.v2/<plugin-id>/` directory.
