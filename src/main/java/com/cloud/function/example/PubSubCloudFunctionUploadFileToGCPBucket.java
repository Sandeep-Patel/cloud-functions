package com.cloud.function.example;

import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.opencsv.CSVWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class PubSubCloudFunctionUploadFileToGCPBucket implements BackgroundFunction<PubSubCloudFunctionUploadFileToGCPBucket.PubSubMessage> {

    private static final Logger logger = Logger.getLogger(PubSubCloudFunctionUploadFileToGCPBucket.class.getName());

    @Override
    public void accept(PubSubMessage message, Context context) {
        if(message.data != null) {
            logger.info(message.data);
            System.out.println(message.data);
            logger.info(write(message.data));
        }
    }


    private static String write(String message) {
        try {
            List<String[]> strings = new ArrayList<>();
            strings.add(message.split(","));
            csvWriterOneByOne(strings, "writtenOneByOne.csv");
            return uploadObject("zooohooo", "sabdeep_test_function_uploads", "writtenOneByOne.csv", "writtenOneByOne.csv");

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
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
                "Dealer Id",
                "Dealer Name",
                "Ad ID",
                "Status"
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

    public static class PubSubMessage {
        String data;
        Map<String, String> attributes;
        String messageId;
        String publishTime;
    }
}
