package com.mapbox.mapboxsdk.offline;

import android.content.Context;
import android.content.ContextWrapper;
import android.database.sqlite.SQLiteDatabase;
import android.os.AsyncTask;
import android.os.Handler;
import android.os.Looper;
import android.text.TextUtils;
import android.util.Log;
import com.mapbox.mapboxsdk.constants.MapboxConstants;
import com.mapbox.mapboxsdk.geometry.CoordinateRegion;
import com.mapbox.mapboxsdk.tileprovider.tilesource.TileDownloadListener;
import com.mapbox.mapboxsdk.util.AppUtils;
import com.mapbox.mapboxsdk.util.DataLoadingUtils;
import com.mapbox.mapboxsdk.util.MapboxUtils;
import com.mapbox.mapboxsdk.util.NetworkUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

public class OfflineMapDownloader implements MapboxConstants {

    private static final String TAG = "OfflineMapDownloader";

    private static OfflineMapDownloader offlineMapDownloader;

    private ArrayList<OfflineMapDownloaderListener> listeners;

    private TileDownloadListener mListener;

    private Context context;

    /**
     * The possible states of the offline map downloader.
     */
    public enum MBXOfflineMapDownloaderState {
        /**
         * An offline map download job is in progress.
         */
        MBXOfflineMapDownloaderStateRunning,
        /**
         * An offline map download job is suspended and can be either resumed or canceled.
         */
        MBXOfflineMapDownloaderStateSuspended,
        /**
         * An offline map download job is being canceled.
         */
        MBXOfflineMapDownloaderStateCanceling,
        /**
         * The offline map downloader is ready to begin a new offline map download job.
         */
        MBXOfflineMapDownloaderStateAvailable
    }

    private class OfflineMapDownloadTaskManager {

        private Iterator<String> itr;
        private int concurrentCount;

        public OfflineMapDownloadTaskManager(Iterator<String> itr, int concurrentCount) {
            this.itr = itr;
            this.concurrentCount = concurrentCount;
        }

        public void start() {
            for (int i = 0; i < concurrentCount; i++) {
                startDownloadTask();
            }
        }

        private void startDownloadTask() {
            if (!itr.hasNext()) {
                return;
            }
/*
            if (!NetworkUtils.isNetworkAvailable(context)) {
                Log.w(TAG, "Network is no longer available.");
//                    [self notifyDelegateOfNetworkConnectivityError:error];
            }
*/
            AsyncTask<String, Void, Void> task = new AsyncTask<String, Void, Void>() {
                @Override
                protected Void doInBackground(String... params) {
                    HttpURLConnection conn = null;
                    String url = params[0];
                    boolean alreadyDownloaded = downloadingDatabase.isURLAlreadyInDatabase(url);
                    if (!alreadyDownloaded) {
                        try {
                            conn = NetworkUtils.getHttpURLConnection(new URL(url));
                            Log.d(TAG, "URL to download = " + conn.getURL().toString());
                            conn.setConnectTimeout(60000);
                            conn.connect();
                            int rc = conn.getResponseCode();
                            if (rc != HttpURLConnection.HTTP_OK) {
                                String msg = String.format(MAPBOX_LOCALE, "HTTP Error connection.  Response Code = %d for url = %s", rc, conn.getURL().toString());
                                Log.w(TAG, msg);
                                notifyDelegateOfHTTPStatusError(rc, params[0]);
                                throw new IOException(msg);
                            }

                            ByteArrayOutputStream bais = new ByteArrayOutputStream();
                            InputStream is = null;
                            try {
                                is = conn.getInputStream();
                                // Read 4K at a time
                                byte[] byteChunk = new byte[4096];
                                int n;

                                while ((n = is.read(byteChunk)) > 0) {
                                    bais.write(byteChunk, 0, n);
                                }
                            } catch (IOException e) {
                                Log.e(TAG, String.format(MAPBOX_LOCALE, "Failed while reading bytes from %s: %s", conn.getURL().toString(), e.getMessage()));
                                e.printStackTrace();
                            } finally {
                                if (is != null) {
                                    is.close();
                                }
                                conn.disconnect();
                            }
                            sqliteSaveDownloadedData(bais.toByteArray(), url);
                            notifyOfDownload();
                        } catch (IOException e) {
                            Log.e(TAG, e.getMessage());
                            e.printStackTrace();
                        } finally {
                            if (conn != null) {
                                conn.disconnect();
                            }
                        }
                    }

                    markOneFileCompleted();
                    startDownloadTask();
                    return null;
                }
            };
            task.execute(itr.next());
        }
    }

    private OfflineMapDatabase downloadingDatabase;
    private MBXOfflineMapDownloaderState state;
    private int totalFilesWritten;
    private int totalFilesExpectedToWrite;


    private ArrayList<OfflineMapDatabase> mutableOfflineMapDatabases;

/*
    // Don't appear to be needed as there's one database per app for offline maps
    @property (nonatomic) NSString *partialDatabasePath;
    @property (nonatomic) NSURL *offlineMapDirectory;

    // Don't appear to be needed as as Android and Mapbox Android SDK provide these
    @property (nonatomic) NSOperationQueue *backgroundWorkQueue;
    @property (nonatomic) NSOperationQueue *sqliteQueue;
    @property (nonatomic) NSURLSession *dataSession;
    @property (nonatomic) NSInteger activeDataSessionTasks;
*/


    private OfflineMapDownloader(Context context) {
        super();
        this.context = context;

        listeners = new ArrayList<OfflineMapDownloaderListener>();

        mutableOfflineMapDatabases = new ArrayList<OfflineMapDatabase>();
        // Load OfflineMapDatabases from File System
        ContextWrapper cw = new ContextWrapper(context);
        for (String s : cw.databaseList()) {
            if (!s.toLowerCase().contains("journal")) {
                // Create the Database Object
                OfflineMapDatabase omd = new OfflineMapDatabase(context, s);
                boolean success = omd.initializeDatabase();
                if (success) {
                    mutableOfflineMapDatabases.add(omd);
                }
            }
        }

        this.state = MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateAvailable;
    }

    private static final Object lock = new Object();

    public static OfflineMapDownloader getOfflineMapDownloader(Context context) {
        synchronized (lock) {
            if (offlineMapDownloader == null) {
                offlineMapDownloader = new OfflineMapDownloader(context);
            }
            return offlineMapDownloader;
        }
    }

    public boolean addOfflineMapDownloaderListener(OfflineMapDownloaderListener listener) {
        return listeners.add(listener);
    }

    public boolean removeOfflineMapDownloaderListener(OfflineMapDownloaderListener listener) {
        return listeners.remove(listener);
    }

/*
    Delegate Notifications
*/

    public void notifyDelegateOfStateChange() {
        for (OfflineMapDownloaderListener listener : listeners) {
            listener.stateChanged(this.state);
        }
    }

    public void notifyDelegateOfInitialCount() {
        for (OfflineMapDownloaderListener listener : listeners) {
            listener.initialCountOfFiles(this.totalFilesExpectedToWrite);
        }
    }

    public void notifyDelegateOfProgress() {
        for (OfflineMapDownloaderListener listener : listeners) {
            listener.progressUpdate(this.totalFilesWritten, this.totalFilesExpectedToWrite);
        }
    }

    public void notifyDelegateOfNetworkConnectivityError(Throwable error) {
        for (OfflineMapDownloaderListener listener : listeners) {
            listener.networkConnectivityError(error);
        }
    }

    public void notifyDelegateOfSqliteError(Throwable error) {
        for (OfflineMapDownloaderListener listener : listeners) {
            listener.sqlLiteError(error);
        }
    }

    public void notifyDelegateOfHTTPStatusError(int status, String url) {
        for (OfflineMapDownloaderListener listener : listeners) {
            listener.httpStatusError(new Exception(String.format(MAPBOX_LOCALE, "HTTP Status Error %d, for url = %s", status, url)));
        }
    }

    public void notifyDelegateOfCompletionWithOfflineMapDatabase(OfflineMapDatabase offlineMap) {
        for (OfflineMapDownloaderListener listener : listeners) {
            listener.completionOfOfflineDatabaseMap(offlineMap);
        }
    }
/*
    Implementation: download urls
*/


    public void startDownloading(final List<String> urls, final OfflineMapURLGenerator generator) {
/*
        // Shouldn't need to check as all downloading will happen in background thread
        if (AppUtils.runningOnMainThread()) {
            Log.w(TAG, "startDownloading() is running on main thread.  Returning.");
            return;
        }
*/
        Log.d(TAG, String.format(MAPBOX_LOCALE, "totalFilesExpectedToWrite = %d, totalFilesWritten = %d", this.totalFilesExpectedToWrite, this.totalFilesWritten));

        final int urlCount = urls.size();
        final int totalCount = urls.size() + generator.getURLCount();
        Iterator<String> urlIter = new Iterator<String>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                synchronized (this) {
                    return index < totalCount;
                }
            }

            @Override
            public String next() {
                synchronized (this) {
                    String toReturn = null;
                    if (index < urlCount) {
                        toReturn = urls.get(index);
                    } else if (index < totalCount) {
                        toReturn = generator.getURLForIndex(context,
                                downloadingDatabase.getMapID(),
                                downloadingDatabase.getImageQuality(),
                                index - urlCount);
                    } else {
                        throw new NoSuchElementException();
                    }
                    index++;
                    return toReturn;
                }
            }

            @Override
            public void remove() {
                synchronized (this) {
                    throw new UnsupportedOperationException();
                }
            }
        };

        if (!urlIter.hasNext()) {
            // All files are downloaded, but hasn't been persisted yet.
            finishUpDownloadProcess();
            return;
        }

        OfflineMapDownloadTaskManager manager = new OfflineMapDownloadTaskManager(urlIter, 8);
        manager.start();
    }

/*
    Implementation: sqlite stuff
*/

    public void sqliteSaveDownloadedData(byte[] data, String url) {
        if (AppUtils.runningOnMainThread()) {
            Log.w(TAG, "trying to run sqliteSaveDownloadedData() on main thread. Return.");
            return;
        }
//        assert(_activeDataSessionTasks > 0);

//        [_sqliteQueue addOperationWithBlock:^{

        // Bail out if the state has changed to canceling, suspended, or available
        //
        if (this.state != MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateRunning) {
            Log.w(TAG, "sqliteSaveDownloadedData() is not in a Running state so bailing.  State = " + this.state);
            return;
        }

        // Open the database read-write and multi-threaded. The slightly obscure c-style variable names here and below are
        // used to stay consistent with the sqlite documentaion.
        // Continue by inserting an image blob into the data table
        //
        this.downloadingDatabase.setURLData(url, data);

/*
        if(error)
        {
            // Oops, that didn't work. Notify the delegate.
            //
            [self notifyDelegateOfSqliteError:error];
        }
        else
        {
*/
/*
        }
*/

        // If this was the last of a batch of urls in the data session's download queue, and there are more urls
        // to be downloaded, get another batch of urls from the database and keep working.
        //
/*
        if(activeDataSessionTasks > 0)
        {
            _activeDataSessionTasks -= 1;
        }
        if(_activeDataSessionTasks == 0 && _totalFilesWritten < _totalFilesExpectedToWrite)
        {
            [self startDownloading];
        }
*/
    }

    private void markOneFileCompleted() {
        // Update the progress
        //
        this.totalFilesWritten += 1;
        notifyDelegateOfProgress();
        Log.d(TAG, "totalFilesWritten = " + this.totalFilesWritten + "; totalFilesExpectedToWrite = " + this.totalFilesExpectedToWrite);

        // If all the downloads are done, clean up and notify the delegate
        //
        if (this.totalFilesWritten >= this.totalFilesExpectedToWrite) {
            finishUpDownloadProcess();
        }
    }

    private void finishUpDownloadProcess() {
        if (this.state == MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateRunning) {
            Log.i(TAG, "Just finished downloading all materials.  Persist the OfflineMapDatabase, change the state, and call it a day.");
            // This is what to do when we've downloaded all the files
            //
            // Populate OfflineMapDatabase object and persist it
            if (!this.mutableOfflineMapDatabases.contains(downloadingDatabase)) {
                this.mutableOfflineMapDatabases.add(downloadingDatabase);
            }

            OfflineMapDatabase finalDatabase = downloadingDatabase;
            this.downloadingDatabase = null;
            notifyDelegateOfCompletionWithOfflineMapDatabase(finalDatabase);

            this.state = MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateAvailable;
            notifyDelegateOfStateChange();
        }
    }

    public boolean sqliteCreateOrUpdateDatabaseUsingMetadata(String mapID, Map<String, String> metadata, List<String> urlStrings, OfflineMapURLGenerator generator) {
        if (AppUtils.runningOnMainThread()) {
            Log.w(TAG, "sqliteCreateOrUpdateDatabaseUsingMetadata() running on main thread.  Returning.");
            return false;
        }

        // Build a query to populate the database (map metadata and list of map resource urls)
        //
/*
        NSMutableString *query = [[NSMutableString alloc] init];
        [query appendString:@"PRAGMA foreign_keys=ON;\n"];
        [query appendString:@"BEGIN TRANSACTION;\n"];
        [query appendString:@"CREATE TABLE metadata (name TEXT UNIQUE, value TEXT);\n"];
        [query appendString:@"CREATE TABLE data (id INTEGER PRIMARY KEY, value BLOB);\n"];
        [query appendString:@"CREATE TABLE resources (url TEXT UNIQUE, status TEXT, id INTEGER REFERENCES data);\n"];
*/
        if (this.downloadingDatabase != null) {
            this.downloadingDatabase.updateMetadata(metadata);
        } else {
            this.downloadingDatabase = createNewDatabase(metadata);
            if (this.downloadingDatabase == null) {
                return false;
            }
        }
        this.totalFilesExpectedToWrite = urlStrings.size() + generator.getURLCount();
        this.totalFilesWritten = 0;
        return true;
/*
        // Open the database read-write and multi-threaded. The slightly obscure c-style variable names here and below are
        // used to stay consistent with the sqlite documentaion.
        sqlite3 *db;
        int rc;
        const char *filename = [_partialDatabasePath cStringUsingEncoding:NSUTF8StringEncoding];
        rc = sqlite3_open_v2(filename, &db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
        if (rc)
        {
            // Opening the database failed... something is very wrong.
            //
            if(error != NULL)
            {
                *error = [NSError mbx_errorCannotOpenOfflineMapDatabase:_partialDatabasePath sqliteError:sqlite3_errmsg(db)];
            }
            sqlite3_close(db);
        }
        else
        {
            // Success! Creating the database file worked, so now populate the tables we'll need to hold the offline map
            //
            const char *zSql = [query cStringUsingEncoding:NSUTF8StringEncoding];
            char *errmsg;
            sqlite3_exec(db, zSql, NULL, NULL, &errmsg);
            if(error && errmsg != NULL)
            {
                *error = [NSError mbx_errorQueryFailedForOfflineMapDatabase:_partialDatabasePath sqliteError:errmsg];
                sqlite3_free(errmsg);
            }
            sqlite3_close(db);
            success = YES;
        }
*/
    }

/*
    API: Begin an offline map download
*/

    private Map<String, String> metadataForNewDatabase(String mapID, boolean includeMarkers, boolean includeMetadata, RasterImageQuality imageQuality) {
        final Hashtable<String, String> metadataDictionary = new Hashtable<String, String>();
        metadataDictionary.put("uniqueID", UUID.randomUUID().toString());
        metadataDictionary.put("mapID", mapID);
        metadataDictionary.put("imageQuality", String.format(MAPBOX_LOCALE, "%d", imageQuality.getValue()));
        metadataDictionary.put("includesMetadata", includeMetadata ? "YES" : "NO");
        metadataDictionary.put("includesMarkers", includeMarkers ? "YES" : "NO");
        return metadataDictionary;
    }

    public OfflineMapDatabase createEmptyMapDatabase(String mapID, RasterImageQuality imageQuality) {
        OfflineMapDatabase db = getOfflineMapDatabaseWithID(mapID);
        if (db != null) {
            if (db.getImageQuality() != imageQuality) {
                Log.w(TAG, "creating (existing) database with mismatched image quality");
                return null;
            } else {
                return db;
            }
        }

        Map<String, String> metadata = metadataForNewDatabase(mapID, false, false, imageQuality);
        db = createNewDatabase(metadata);
        if (db != null && !mutableOfflineMapDatabases.contains(db)) {
            mutableOfflineMapDatabases.add(db);
        }
        return db;
    }

    public void beginDownloadingMapID(String mapID, CoordinateRegion mapRegion, Integer minimumZ, Integer maximumZ) {
        beginDownloadingMapID(mapID, mapRegion, minimumZ, maximumZ, true, true, RasterImageQuality.MBXRasterImageQualityFull);
    }

    public void beginDownloadingMapID(String mapID, CoordinateRegion mapRegion, Integer minimumZ, Integer maximumZ, boolean includeMetadata, boolean includeMarkers) {
        beginDownloadingMapID(mapID, mapRegion, minimumZ, maximumZ, includeMetadata, includeMarkers, RasterImageQuality.MBXRasterImageQualityFull);
    }

    public void beginDownloadingMapID(final String mapID, CoordinateRegion mapRegion, Integer minimumZ, Integer maximumZ,
                                      boolean includeMetadata, boolean includeMarkers, RasterImageQuality imageQuality) {
        if (state != MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateAvailable) {
            Log.w(TAG, "state doesn't equal MBXOfflineMapDownloaderStateAvailable so return.  state = " + state);
            return;
        }

//        [self setUpNewDataSession];

//        [_backgroundWorkQueue addOperationWithBlock:^{

        // Start a download job to retrieve all the resources needed for using the specified map offline
        //
        this.state = MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateRunning;

        this.downloadingDatabase = getOfflineMapDatabaseWithID(mapID);
        final Map<String, String> metadataDictionary;
        if (this.downloadingDatabase == null) {
            metadataDictionary = metadataForNewDatabase(mapID, includeMarkers, includeMetadata, imageQuality);
        } else {
            Hashtable<String, String> makingMetadata = new Hashtable<String, String>();
            RasterImageQuality previousImageQuality = this.downloadingDatabase.getImageQuality();
            if (previousImageQuality != imageQuality) {
                String errorString = String.format(MAPBOX_LOCALE,
                        "Incompatible imageQuality parameters: %s in db, trying to download with %s",
                        previousImageQuality.toString(),
                        imageQuality.toString());
                cancelImmediatelyWithError(errorString);
                return;
            }
            boolean didIncludeMarkers = this.downloadingDatabase.includesMarkers();
            boolean didIncludeMetadata = this.downloadingDatabase.includesMetadata();

            if (!didIncludeMarkers && includeMarkers) {
                makingMetadata.put("includesMarkers", "YES");
            } else {
                // Don't download the data if it's already in the db or not requested
                includeMarkers = false;
            }

            if (!didIncludeMetadata && includeMetadata) {
                makingMetadata.put("includesMetadata", "YES");
            } else {
                // Don't download the data if it's already in the db or not requested
                includeMetadata = false;
            }
            metadataDictionary = makingMetadata;
        }

        /*
        this.uniqueID = UUID.randomUUID().toString();
        this.mapID = mapID;
        this.includesMetadata = includeMetadata;
        this.includesMarkers = includeMarkers;
        this.imageQuality = imageQuality;
        this.mapRegion = mapRegion;*/

//        [self notifyDelegateOfStateChange];

        final ArrayList<String> urls = new ArrayList<String>();

        String dataName = "features.json";    // Only using API V4 for now

        // Include URLs for the metadata and markers json if applicable
        //
        if (includeMetadata) {
            urls.add(String.format(MAPBOX_LOCALE, MAPBOX_BASE_URL_V4 + "%s.json?secure&access_token=%s", mapID, MapboxUtils.getAccessToken()));
        }
        if (includeMarkers) {
            urls.add(String.format(MAPBOX_LOCALE, MAPBOX_BASE_URL_V4 + "%s/%s?access_token=%s", mapID, dataName, MapboxUtils.getAccessToken()));
        }

        // Loop through the zoom levels and lat/lon bounds to generate a list of urls which should be included in the offline map
        //
        double minLat = mapRegion.getCenter().getLatitude() - (mapRegion.getSpan().getLatitudeSpan() / 2.0);
        double maxLat = minLat + mapRegion.getSpan().getLatitudeSpan();
        double minLon = mapRegion.getCenter().getLongitude() - (mapRegion.getSpan().getLongitudeSpan() / 2.0);
        double maxLon = minLon + mapRegion.getSpan().getLongitudeSpan();
        final OfflineMapURLGenerator generator = new OfflineMapURLGenerator(minLat, maxLat, minLon, maxLon, minimumZ, maximumZ);
        Log.i(TAG, "Number of URLs so far: " + (urls.size() + generator.getURLCount()));

        // Determine if we need to add marker icon urls (i.e. parse markers.geojson/features.json), and if so, add them
        //
        if (includeMarkers) {
            String dName = "markers.geojson";
            final String geojson = String.format(MAPBOX_LOCALE, MAPBOX_BASE_URL_V4 + "%s/%s?access_token=%s", mapID, dName, MapboxUtils.getAccessToken());

            if (!NetworkUtils.isNetworkAvailable(context)) {
                // We got a session level error which probably indicates a connectivity problem such as airplane mode.
                // Since we must fetch and parse markers.geojson/features.json in order to determine which marker icons need to be
                // added to the list of urls to download, the lack of network connectivity is a non-recoverable error
                // here.
                //
                // TODO
/*
                [self notifyDelegateOfNetworkConnectivityError:error];
                [self cancelImmediatelyWithError:error];
*/
                return;
            }

            AsyncTask<Void, Void, Void> foo = new AsyncTask<Void, Void, Void>() {
                @Override
                protected Void doInBackground(Void... params) {
                    try {
                        HttpURLConnection conn = NetworkUtils.getHttpURLConnection(new URL(geojson));
                        conn.setConnectTimeout(60000);
                        conn.connect();
                        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                            throw new IOException();
                        }

                        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream(), Charset.forName("UTF-8")));
                        String jsonText = DataLoadingUtils.readAll(rd);

                        // The marker geojson was successfully retrieved, so parse it for marker icons. Note that we shouldn't
                        // try to save it here, because it may already be in the download queue and saving it twice will mess
                        // up the count of urls to be downloaded!
                        //
                        Set<String> markerIconURLStrings = new HashSet<String>();
                        markerIconURLStrings.addAll(parseMarkerIconURLStringsFromGeojsonData(jsonText));
                        Log.i(TAG, "Number of markerIconURLs = " + markerIconURLStrings.size());
                        if (markerIconURLStrings.size() > 0) {
                            urls.addAll(markerIconURLStrings);
                        }
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        // The url for markers.geojson/features.json didn't work (some maps don't have any markers). Notify the delegate of the
                        // problem, and stop attempting to add marker icons, but don't bail out on whole the offline map download.
                        // The delegate can decide for itself whether it wants to continue or cancel.
                        //
                        // TODO
                        e.printStackTrace();
/*
                        [self notifyDelegateOfHTTPStatusError:((NSHTTPURLResponse *)response).statusCode url:response.URL];
*/
                    }
                    return null;
                }

                @Override
                protected void onPostExecute(Void aVoid) {
                    super.onPostExecute(aVoid);
                    Log.i(TAG, "Done figuring out marker icons, so now start downloading everything.");

                    // ==========================================================================================================
                    // == WARNING! WARNING! WARNING!                                                                           ==
                    // == This stuff is a duplicate of the code immediately below it, but this copy is inside of a completion  ==
                    // == block while the other isn't. You will be sad and confused if you try to eliminate the "duplication". ==
                    //===========================================================================================================
                    startDownloadProcess(mapID, metadataDictionary, urls, generator);
                }
            };
            foo.execute();
        } else {
            Log.i(TAG, "No marker icons to worry about, so just start downloading.");
            // There aren't any marker icons to worry about, so just create database and start downloading
            startDownloadProcess(mapID, metadataDictionary, urls, generator);
        }
    }

    private OfflineMapDatabase createNewDatabase(Map<String, String> metadata) {
        String mapID = metadata.get("mapID");
        OfflineMapDatabase db = new OfflineMapDatabase(context, mapID, metadata);
        boolean initialized = db.initializeDatabase();
        if (!initialized) {
            String dbPath = db.getPath();
            db.closeDatabase();
            if (dbPath != null) {
                File dbFile = new File(dbPath);
                dbFile.delete();
            }
            return null;
        } else {
            return db;
        }
    }

    /**
     * Private method for Starting the Whole Download Process
     *
     * @param metadata Metadata
     * @param urls     Map urls
     */
    private void startDownloadProcess(final String mapID, final Map<String, String> metadata, final List<String> urls, final OfflineMapURLGenerator generator) {
        AsyncTask<Void, Void, Thread> startDownload = new AsyncTask<Void, Void, Thread>() {
            @Override
            protected Thread doInBackground(Void... params) {
                // Do database creation / io on background thread
                if (!sqliteCreateOrUpdateDatabaseUsingMetadata(mapID, metadata, urls, generator)) {
                    cancelImmediatelyWithError("Map Database wasn't created");
                    return null;
                }
                notifyDelegateOfInitialCount();
                startDownloading(urls, generator);
                return null;
            }

        };

        // Create the database and start the download
        startDownload.execute();
    }


    public Set<String> parseMarkerIconURLStringsFromGeojsonData(String data) {
        HashSet<String> iconURLStrings = new HashSet<String>();

        JSONObject simplestyleJSONDictionary = null;
        try {
            simplestyleJSONDictionary = new JSONObject(data);

            // Find point features in the markers dictionary (if there are any) and add them to the map.
            //
            JSONArray markers = simplestyleJSONDictionary.getJSONArray("features");

            if (markers != null && markers.length() > 0) {
                for (int lc = 0; lc < markers.length(); lc++) {
                    Object value = markers.get(lc);
                    if (value instanceof JSONObject) {
                        JSONObject feature = (JSONObject) value;
                        String type = feature.getJSONObject("geometry").getString("type");

                        if ("Point".equals(type)) {
                            String size = feature.getJSONObject("properties").getString("marker-size");
                            String color = feature.getJSONObject("properties").getString("marker-color");
                            String symbol = feature.getJSONObject("properties").getString("marker-symbol");
                            if (!TextUtils.isEmpty(size) && !TextUtils.isEmpty(color) && !TextUtils.isEmpty(symbol)) {
                                String markerURL = MapboxUtils.markerIconURL(context, size, symbol, color);
                                if (!TextUtils.isEmpty(markerURL)) {
                                    iconURLStrings.add(markerURL);

                                }
                            }
                        }
                    }
                    // This is the last line of the loop
                }
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        // Return only the unique icon urls
        //
        return iconURLStrings;
    }

    public void cancelImmediatelyWithError(String error) {
        if (downloadingDatabase != null) {
            downloadingDatabase.closeDatabase();
            downloadingDatabase = null;
        }

        // TODO
/*
        // Creating the database failed for some reason, so clean up and change the state back to available
        //
        state = MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateCanceling;
        [self notifyDelegateOfStateChange];

        if([_delegate respondsToSelector:@selector(offlineMapDownloader:didCompleteOfflineMapDatabase:withError:)])
        {
            dispatch_async(dispatch_get_main_queue(), ^(void){
                    [_delegate offlineMapDownloader:self didCompleteOfflineMapDatabase:nil withError:error];
            });
        }

        [_dataSession invalidateAndCancel];
        [_sqliteQueue cancelAllOperations];

        [_sqliteQueue addOperationWithBlock:^{
        [self setUpNewDataSession];
        _totalFilesWritten = 0;
        _totalFilesExpectedToWrite = 0;

        [[NSFileManager defaultManager] removeItemAtPath:_partialDatabasePath error:nil];

        state = MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateAvailable;
        [self notifyDelegateOfStateChange];
    }];
*/
    }

/*
    API: Control an in-progress offline map download
*/

    public void cancel() {
        Log.d(TAG, "cancel called with state = " + state);
/*
        if (state != MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateCanceling && state != MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateAvailable) {
            // Stop a download job and discard the associated files
            //
            [_backgroundWorkQueue addOperationWithBlock:^{
            _state = MBXOfflineMapDownloaderStateCanceling;
            [self notifyDelegateOfStateChange];

            [_dataSession invalidateAndCancel];
            [_sqliteQueue cancelAllOperations];

            [_sqliteQueue addOperationWithBlock:^{
                [self setUpNewDataSession];
                _totalFilesWritten = 0;
                _totalFilesExpectedToWrite = 0;
                [[NSFileManager defaultManager] removeItemAtPath:_partialDatabasePath error:nil];

                if([_delegate respondsToSelector:@selector(offlineMapDownloader:didCompleteOfflineMapDatabase:withError:)])
                {
                    NSError *canceled = [NSError mbx_errorWithCode:MBXMapKitErrorCodeDownloadingCanceled reason:@"The download job was canceled" description:@"Download canceled"];
                    dispatch_async(dispatch_get_main_queue(), ^(void){
                            [_delegate offlineMapDownloader:self didCompleteOfflineMapDatabase:nil withError:canceled];
                    });
                }

                _state = MBXOfflineMapDownloaderStateAvailable;
                [self notifyDelegateOfStateChange];
            }];

            }
        }
*/
    }

    public void resume() {
        if (state != MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateSuspended) {
            return;
        }
/*
        // Resume a previously suspended download job
        //
        [_backgroundWorkQueue addOperationWithBlock:^{
            _state = MBXOfflineMapDownloaderStateRunning;
            [self startDownloading];
            [self notifyDelegateOfStateChange];
        }];
*/
    }

    public void suspend() {
        Log.d(TAG, "suspend called with state = " + state);
/*
        if (state == MBXOfflineMapDownloaderState.MBXOfflineMapDownloaderStateRunning) {
            // Stop a download job, preserving the necessary state to resume later
            //
            [_backgroundWorkQueue addOperationWithBlock:^{
                [_sqliteQueue cancelAllOperations];
                _state = MBXOfflineMapDownloaderStateSuspended;
                _activeDataSessionTasks = 0;
                [self notifyDelegateOfStateChange];
            }];
        }
*/
    }

/*
    API: Access or delete completed offline map databases on disk
*/

    public ArrayList<OfflineMapDatabase> getMutableOfflineMapDatabases() {
        // Return an array with offline map database objects representing each of the *complete* map databases on disk
        return mutableOfflineMapDatabases;
    }

    public boolean isMapIdAlreadyAnOfflineMapDatabase(String mapId) {
        return getOfflineMapDatabaseWithID(mapId) != null;
    }

    public boolean removeOfflineMapDatabase(OfflineMapDatabase offlineMapDatabase) {
        // Mark the offline map object as invalid in case there are any references to it still floating around
        //
        String dbPath = offlineMapDatabase.getPath();
        offlineMapDatabase.invalidate();

        // Remove the offline map object from the array and delete its backing database
        //
        mutableOfflineMapDatabases.remove(offlineMapDatabase);

        // Remove Offline Database SQLite file
        //
        File dbFile = new File(dbPath);
        boolean result = dbFile.delete();
        Log.i(TAG, String.format(MAPBOX_LOCALE, "Result of removing database file: %s", result));
        return result;
    }

    public boolean removeOfflineMapDatabaseWithID(String mid) {
        OfflineMapDatabase database = getOfflineMapDatabaseWithID(mid);
        if (database != null) {
            return removeOfflineMapDatabase(database);
        }
        return false;
    }

    public OfflineMapDatabase getOfflineMapDatabaseWithID(String mid) {
        for (OfflineMapDatabase database : getMutableOfflineMapDatabases()) {
            if (database.getMapID().equals(mid)) {
                return database;
            }
        }
        return null;
    }

    private void notifyOfDownload() {
        (new Handler(Looper.getMainLooper())).post(new Runnable() {
            @Override
            public void run() {
                if (mListener != null) {
                    mListener.singleTileDownloaded();
                }
            }
        });
    }

    public void setDownloadListener(TileDownloadListener l) {
        mListener = l;
    }

}
