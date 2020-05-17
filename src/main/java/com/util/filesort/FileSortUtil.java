package com.util.filesort;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class FileSortUtil {
    public static void main(String[] args) throws Exception {
        System.out.println("****************************************************************");
        System.out.println("*************Welcome, Let's collect sort job parameters ****");
        System.out.println("****************************************************************");
        System.out.println("\n");
        System.out.println("\n");

        System.out.println("***********(1) Input path of the file to be sorted***********\n");
        Scanner readParam = new Scanner(System.in);
        String inputFilePath = readParam.nextLine();
        if(inputFilePath == null || "".equalsIgnoreCase(inputFilePath))
        {
            System.err.println("No value supplied for file input path. Exiting");
        }
        if(!(inputFilePath.endsWith("csv") || inputFilePath.endsWith("CSV")))
        {
            System.err.println("Must be a csv file. Exiting");
            System.exit(1);
        }
        System.out.println("***********(2) Max number of records can be loaded in memory***********\n");
        Integer num_records_in_memory = readParam.nextInt();

        System.out.println("***********(3) Output directory***********\n");
        String tempDirectory = readParam.next();

        System.out.println("***********(4) Index of sort column[** first column has index 0]***********\n");
        int sortKeyIndex = readParam.nextInt();

        long startTime = System.currentTimeMillis();
        int lines = 0;
        System.out.println("Cleaning temp directory..");
        cleanupTempDir(tempDirectory);

        List<String> smallFileContents = new ArrayList<>();
        try (InputStream fileInputStream = new FileInputStream(new File(inputFilePath))) {
            Scanner inputFileScanner = new Scanner(fileInputStream);
            while (inputFileScanner.hasNextLine()) {
                String data = inputFileScanner.nextLine();
                smallFileContents.add(data);
                if (++lines == num_records_in_memory) {
                    createSortedSmallFile(smallFileContents, sortKeyIndex, tempDirectory);
                    smallFileContents.clear();
                    lines = 0;
                }
            }
        }
        System.out.println("merging sorted small files");
        mergeAndSortSmallFiles(tempDirectory, sortKeyIndex);
        System.out.println("time taken: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds");
    }

    private static void cleanupTempDir(String tempDirectory) {
        File tempFolder = new File(tempDirectory);
        if (tempFolder.isDirectory()) {
            Arrays.asList(tempFolder.listFiles()).forEach(File::delete);
        }
    }

    private static void mergeAndSortSmallFiles(String tempDirectory, int sortKeyIndex) throws Exception {
        File tempDir = new File(tempDirectory);
        if (tempDir.isDirectory()) {
            int numFiles = 0;
            do {
                String[] files = tempDir.list();
                numFiles = files.length;
                if (numFiles > 1) {
                    mergeFileContents(Paths.get(tempDirectory + File.separator + files[0]),
                            Paths.get(tempDirectory + File.separator + files[1]),
                            tempDirectory,
                            sortKeyIndex);
                }
            } while (numFiles > 1);
            File[] outputFile = tempDir.listFiles();
            if(outputFile.length == 1){
                outputFile[0].renameTo(Paths.get(tempDirectory+"SortedOutput.csv").toFile());
            }
        }
    }

    static void mergeFileContents(Path file1, Path file2, String tempDirectoryPath, int keyIndexToSort) throws Exception {
        //path for merged output file
        String outputPath = tempDirectoryPath + File.separator + UUID.randomUUID();
        boolean addNewLine = false;
        try (FileOutputStream fos = new FileOutputStream(outputPath);
             BufferedWriter outputFileWriter = new BufferedWriter(new OutputStreamWriter(fos))) {
            try (Scanner inputFileScanner1 = new Scanner(new FileInputStream(file1.toFile()));
                 Scanner inputFileScanner2 = new Scanner(new FileInputStream(file2.toFile()))) {
                String file1Content = inputFileScanner1.nextLine();
                String file2Content = inputFileScanner2.nextLine();
                while (file1Content == null || "".equalsIgnoreCase(file1Content)) {
                    file1Content = inputFileScanner1.nextLine();
                }
                while (file2Content == null || "".equalsIgnoreCase(file2Content)) {
                    file2Content = inputFileScanner2.nextLine();
                }

                String[] file1Tokens = file1Content.split(",");
                String[] file2Tokens = file2Content.split(",");
                boolean file1ReadRemaining = true;
                boolean file2ReadRemaining = true;

                //Until both files have sorted content
                do {
                    if (file1Tokens[keyIndexToSort].compareTo(file2Tokens[keyIndexToSort]) > 0) {
                        if (addNewLine) {
                            outputFileWriter.newLine();
                        } else {
                            addNewLine = true;
                        }
                        outputFileWriter.write(file2Content);
                        file2ReadRemaining = inputFileScanner2.hasNext();
                        if (file2ReadRemaining) {
                            file2Content = inputFileScanner2.nextLine();
                            file2Tokens = file2Content.split(",");
                        }
                    } else {

                        if (addNewLine) {
                            outputFileWriter.newLine();
                        } else {
                            addNewLine = true;
                        }
                        outputFileWriter.write(file1Content);
                        file1ReadRemaining = inputFileScanner1.hasNext();
                        if (file1ReadRemaining) {
                            file1Content = inputFileScanner1.nextLine();
                            file1Tokens = file1Content.split(",");
                        }
                    }
                } while (file1ReadRemaining && file2ReadRemaining);
                //write one which was not written since check above failed
                if (file1ReadRemaining) {
                    if (addNewLine) {
                        outputFileWriter.newLine();
                    } else {
                        addNewLine = true;
                    }
                    outputFileWriter.write(file1Content);
                    //Write remaining lines from file 1
                    while (inputFileScanner1.hasNextLine()) {
                        file1Content = inputFileScanner1.nextLine();
                        if (addNewLine) {
                            outputFileWriter.newLine();
                        } else {
                            addNewLine = true;
                        }
                        outputFileWriter.write(file1Content);

                    }
                }
                if (file2ReadRemaining) {
                    if (addNewLine) {
                        outputFileWriter.newLine();
                    } else {
                        addNewLine = true;
                    }
                    outputFileWriter.write(file2Content);
                    // write remaining lines from file 2
                    while (inputFileScanner2.hasNextLine()) {
                        file2Content = inputFileScanner2.nextLine();
                        if (addNewLine) {
                            outputFileWriter.newLine();
                        } else {
                            addNewLine = true;
                        }
                        outputFileWriter.write(file2Content);
                    }
                }
            }
        }
        //Cleanup already merged files
        Files.deleteIfExists(file1);
        Files.deleteIfExists(file2);

    }

    private static void createSortedSmallFile(List<String> smallFileContents,
                                              int sortKeyIndex, String tempDirectory)
            throws Exception {

        Map<String, List<String>> holder = smallFileContents.stream()
                .map(x -> {
                    ArrayList<String> tempData = new ArrayList<>();
                    tempData.add(x);
                    String[] tokens = x.split(",");
                    return new AbstractMap.SimpleEntry<String, List<String>>(tokens[sortKeyIndex], tempData);
                })
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey,
                        AbstractMap.SimpleEntry::getValue,
                        (value1, value2) -> {
                            value1.addAll(value2);
                            return value1;
                        })
                );

        List<String> sortedList = new TreeMap<>(holder)
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());


        UUID fileId = UUID.randomUUID();
        String tempFilePath = tempDirectory + File.separator + fileId;
        Files.write(Paths.get(tempFilePath), sortedList);


    }
}
