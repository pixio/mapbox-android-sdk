package com.mapbox.mapboxsdk.tileprovider.tilesource;

import com.mapbox.mapboxsdk.offline.OfflineMapDatabase;

public interface TileDownloadListener {
    public void singleTileDownloaded();
    public void tileDownloadedOrChecked(String url, OfflineMapDatabase db);
}
