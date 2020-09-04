package com.cloud.function.example;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.opencsv.CSVWriter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class HttpCloudFunctionUploadFileToGCPBucket implements HttpFunction {

    @Override
    public void service(HttpRequest request, HttpResponse response) throws Exception {
        BufferedWriter writer = response.getWriter();
        writer.write(HttpCloudFunctionUploadFileToGCPBucket.write());
    }

    public static void main(String[] args) {
        try {
            write();
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }
    }
    private static String write() throws Exception {
        List<String[]> strings = new ArrayList<>();
        String[] a = {"A","","C"};
        String[] b = {"","B","C"};
        strings.add(a);
        strings.add(b);

        csvWriterOneByOne(strings,"writtenOneByOne.csv");
        return uploadObject("zooohooo","sabdeep_test_function_uploads", "writtenOneByOne.csv", "writtenOneByOne.csv");
    }

    public static String uploadObject(
            String projectId, String bucketName, String objectName, String filePath) throws IOException {
        // The ID of your GCP project
        // String projectId = "your-project-id";

        // The ID of your GCS bucket
        // String bucketName = "your-unique-bucket-name";

        // The ID of your GCS object
        // String objectName = "your-object-name";

        // The path to your file to upload
        // String filePath = "path/to/your/file"
        String tempDirectory = System.getProperty("java.io.tmpdir");
        System.out.println("Temp dir: " + tempDirectory);
        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(tempDirectory + "/"+filePath)));

        return "File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName;
    }

    public static String[] getHeader(){
        return new String[]{
                "Token Id",
                "PNREF",
                "Card Hint"
        };
    }

    public static void csvWriterOneByOne(List<String[]> stringArray, String fileName) throws Exception {
        String tempDirectory = System.getProperty("java.io.tmpdir");
        CSVWriter csvWriter = null;
        try (FileWriter writer = new FileWriter(tempDirectory + "/" +fileName)) {
            csvWriter = new CSVWriter(writer);
            csvWriter.writeNext(getHeader());

            for (String[] array : stringArray) {
                csvWriter.writeNext(array);
            }
            csvWriter.close();
        }
    }
}
