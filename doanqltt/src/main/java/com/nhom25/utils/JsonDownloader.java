package com.nhom25.utils;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.io.IOException;

public class JsonDownloader {
    public static void downloadJson(String apiUrl, String fileName) throws IOException {
        URL url = URI.create(apiUrl).toURL();
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        connection.setRequestMethod("GET");

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_OK) {

            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();

            try (FileWriter fileWriter = new FileWriter(fileName)) {
                fileWriter.write(content.toString());
                System.out.println("Saved " + fileName + " successfully.");
            }
        } else {
            System.out.println("Failed to connect. Response code: " + responseCode);
        }

        connection.disconnect();
    }
}
