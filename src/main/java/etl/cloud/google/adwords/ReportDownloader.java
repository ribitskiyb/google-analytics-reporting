package etl.cloud.google.adwords;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.Analytics.Data.Ga.Get;
import com.google.api.services.analytics.AnalyticsScopes;
import com.google.api.services.analytics.model.GaData;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.isBlank;


public class ReportDownloader {

    public ReportDownloader(String p12KeyFilePath, String serviceAccountId, String applicationName)
            throws GeneralSecurityException, IOException {
        // Initialize an authorized analytics service object.

        JsonFactory jsonFactory = GsonFactory.getDefaultInstance();

        // Construct a GoogleCredential object with the service account email
        // and p12 file downloaded from the developer console.
        HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        GoogleCredential credential = new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(jsonFactory)
                .setServiceAccountId(serviceAccountId)
                .setServiceAccountPrivateKeyFromP12File(new File(p12KeyFilePath))
                .setServiceAccountScopes(AnalyticsScopes.all())
                .build();

        // Construct the Analytics service object.
        this.analytics = new Analytics.Builder(httpTransport, jsonFactory, credential)
                .setApplicationName(applicationName)
                .build();
    }

    public void download(String ids, String startDate, String endDate, String metrics,
                         String dimensions, String sort, String filters, String segment, String samplingLevel,
                         String outputFile) throws IOException {

        // Fail early if outputFile can't be written
        try (FileWriter fw = new FileWriter(outputFile)) {
            fw.write("");
        }

        Get request = buildRequest(ids, startDate, endDate, metrics, dimensions, sort, filters, segment, samplingLevel);

        List<List<String>> data = fetchData(request);

        writeCSV(data, outputFile, ',');
    }

    private static final int MAX_RESULTS_PER_REQUEST = (int) 1e4;  // API limit on amount of rows returned for a single request

    private Analytics analytics;
    
    private Get buildRequest(String ids, String startDate, String endDate, String metrics,
                             String dimensions, String sort, String filters, String segment, String samplingLevel)
            throws IOException {
        Get request = analytics.data().ga()
                .get(ids, startDate, endDate, metrics);

        if (!isBlank(dimensions))    request.setDimensions(dimensions);
        if (!isBlank(sort))          request.setSort(sort);
        if (!isBlank(filters))       request.setFilters(filters);
        if (!isBlank(segment))       request.setSegment(segment);
        if (!isBlank(samplingLevel)) request.setSamplingLevel(samplingLevel);

        return request;
    }

    // Fetches paginated data into a single collection
    private List<List<String>> fetchData(Get request) throws IOException {
        List<List<String>> fetched = new ArrayList<>();

        request.setMaxResults(MAX_RESULTS_PER_REQUEST);
        request.setStartIndex(1);

        GaData page;
        while (true) {
            page = request.execute();
            fetched.addAll(page.getRows());

            int fetchedRows = fetched.size();
            int totalRows = page.getTotalResults();
            if (fetchedRows >= totalRows)
                break;
            request.setStartIndex(fetchedRows + 1);
        }

        // Put header row
        List<String> headers = new ArrayList<>();
        page.getColumnHeaders().forEach( h -> headers.add(h.getName()) );
        fetched.add(0, headers);

        return fetched;
    }

    private void writeCSV(List<List<String>> data, String path, char separator) throws IOException {
        try (CSVWriter writer = new CSVWriter(new FileWriter(path), separator)) {
            for (List<String> row : data) {
                if (row == null)
                    continue;
                writer.writeNext(row.toArray(new String[0]));
            }
        }
    }

}
