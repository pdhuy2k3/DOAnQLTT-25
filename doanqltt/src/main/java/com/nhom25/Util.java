package com.nhom25;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Util {
    public static Map<String, String> parseArgs(String[] args) {
        return IntStream.range(0, args.length)
                .filter(i -> i % 2 == 0) // Chỉ lấy các chỉ số chẵn (là các keys như --tblName, --executionDate)
                .boxed()
                .collect(Collectors.toMap(
                        i -> args[i],                 // Key là args[i] (ví dụ: --tblName)
                        i -> (i + 1 < args.length) ? args[i + 1] : null // Value là args[i + 1], nếu tồn tại
                ));
    }

    // Hàm lấy ngày hiện tại dưới định dạng yyyy-MM-dd
    public static String getCurrentDate() {
        LocalDate currentDate = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return currentDate.format(formatter);
    }

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
