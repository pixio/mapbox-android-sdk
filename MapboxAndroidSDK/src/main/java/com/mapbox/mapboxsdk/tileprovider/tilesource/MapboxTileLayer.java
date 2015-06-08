package com.mapbox.mapboxsdk.tileprovider.tilesource;

import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.text.TextUtils;
import android.util.Log;

import com.mapbox.mapboxsdk.constants.MapboxConstants;
import com.mapbox.mapboxsdk.offline.OfflineMapDatabase;
import com.mapbox.mapboxsdk.offline.OfflineMapDownloader;
import com.mapbox.mapboxsdk.util.MapboxUtils;
import com.mapbox.mapboxsdk.util.NetworkUtils;
import com.mapbox.mapboxsdk.views.util.constants.MapViewConstants;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Locale;

/**
 * A convenience class to initialize tile layers that use Mapbox services.
 * Underneath, this initializes a WebSourceTileLayer, but provides conveniences
 * for retina tiles, initialization by ID, and loading over SSL.
 */
public class MapboxTileLayer extends TileJsonTileLayer implements MapViewConstants, MapboxConstants {
    private static final String TAG = "MapboxTileLayer";
    private String mId;
    private Context mContext;
    private OfflineMapDatabase mOfflineDatabase;
    private final Object lock = new Object();

    /**
     * Initialize a new tile layer, directed at a hosted Mapbox tilesource.
     *
     * @param mapId a valid mapid, of the form account.map
     */
    public MapboxTileLayer(Context context, String mapId) {
        this(context, mapId, true);
    }

    public MapboxTileLayer(Context context, String mapId, boolean enableSSL) {
        super(mapId, mapId, enableSSL);
        mContext = context;
    }

    @Override
    protected void initialize(String pId, String aUrl, boolean enableSSL) {
        mId = pId;
        super.initialize(pId, aUrl, enableSSL);
    }

    @Override
    public TileLayer setURL(final String aUrl) {
        if (!TextUtils.isEmpty(aUrl) && !aUrl.toLowerCase(Locale.US).contains("http://") && !aUrl.toLowerCase(Locale.US).contains("https://")) {
            super.setURL(MAPBOX_BASE_URL_V4 + aUrl + "/{z}/{x}/{y}{2x}.png?access_token=" + MapboxUtils.getAccessToken());
        } else {
            super.setURL(aUrl);
        }
        return this;
    }

    @Override
    protected String getBrandedJSONURL() {
        String url = String.format(MAPBOX_LOCALE, MAPBOX_BASE_URL_V4 + "%s.json?access_token=%s&secure=1", mId, MapboxUtils.getAccessToken());
        if (!mEnableSSL) {
            url = url.replace("https://", "http://");
            url = url.replace("&secure=1", "");
        }

        return url;
    }

    public String getCacheKey() {
        return mId;
    }

    public Bitmap internalGetBitmapFromURL(String url) {
        try {
            OfflineMapDatabase db = getOfflineDatabase();
            byte[] data = null;
            if (db != null) {
                data = db.sqliteDataForURL(url);
            }

            if (data == null) {
                HttpURLConnection connection = NetworkUtils.getHttpURLConnection(new URL(url));
                data = readFully(connection.getInputStream());
                if (db != null) {
                    db.setURLData(url, data);
                }
            }

            if (data != null) {
                return BitmapFactory.decodeByteArray(data, 0, data.length);
            } else {
                return null;
            }
        } catch (final Throwable e) {
            Log.e(TAG, "Error downloading MapTile: " + url + ":" + e);
            return null;
        }
    }

    @Override
    public void detach() {
        if (mOfflineDatabase != null) {
            mOfflineDatabase.closeDatabase();
        }
    }

    private OfflineMapDatabase getOfflineDatabase() {
        synchronized (lock) {
            if (mOfflineDatabase == null) {
                OfflineMapDownloader downloader = OfflineMapDownloader.getOfflineMapDownloader(mContext);
                mOfflineDatabase = downloader.getOfflineMapDatabaseWithID(mId);
            }
            return mOfflineDatabase;
        }
    }

}
