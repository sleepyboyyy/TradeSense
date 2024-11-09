package mk.tradesense.tradesense;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;

@Component
public class DataLoader {
    @PostConstruct
    public void init() {
        try {
            // Retrieve the Python path from environment variables
            String pythonPath = System.getenv("PYTHON_PATH");
            if (pythonPath == null) {
                throw new IllegalStateException("PYTHON_PATH environment variable is not set");
            }

            ProcessBuilder processBuilder = new ProcessBuilder(pythonPath, "src/main/java/mk/tradesense/tradesense/scripts/data_scraper_v2.py");
            processBuilder.redirectErrorStream(true);
            Process process = processBuilder.start();

            // Capture and print the output from the script
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            }

            int exitCode = process.waitFor();
            System.out.println("Python script exited with code: " + exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
