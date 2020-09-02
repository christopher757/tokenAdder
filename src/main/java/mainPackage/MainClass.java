package mainPackage;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


@Slf4j
public class MainClass {


    @Option(name = "--data", required = true, usage = "Data folder")
    private static String data = "";

    @Option(name = "--tokenFile", required = false, usage = "Token file")
    private static String tokenFile = "";

    @Option(name = "--mappings", required = false, usage = "Header mappings. All mappings should be: source1,target1(new row); source2,target2.")
    private static String settings = "";

    @Option(name = "--existingHeaders", required = false, usage = "Existing headers.")
    private static String headers = "";

    @Option(name = "--isList", required = false, usage = "Whether the data is in a list format")
    private static String isList = "";

    @Option(name = "--tokenHeaders", required = false, usage = "Manually define token headers")
    private static String predefinedTokenHeaders = "";

    @Option(name = "--incomplete", usage = "Deletes all rows, that don't have any of the mappings available.")
    private static String incomplete = "false";

    @Option(name = "--newRows", usage = "Create new rows with a default value. Format -> newHeader1, defaultvalue1, newHeader2, defaultvalue2")
    private static String newRows = "false";

    private static boolean list = false;

    private static boolean needsHeaders = false;

    private static List<String> newHeaders = new ArrayList<>();

    private static List<String> newValues = new ArrayList<>();

    private static boolean fileHasHeaders = false;

    private static List<String> existingHeaders = new ArrayList<>();

    private static String[] existingHeaderArray;

    private static String[] tokenHeaderArray;

    private static Map<String, Map<String, String>> mappings = new TreeMap<>(); //They are formed as follows:  tokenHeader -> {originalRow -> tokenRow}

    private static List<String> tokenHeaderList = new ArrayList<>();

    private static List<String> targets = new ArrayList<>();

    private static Map<String, Integer> mapping = new HashMap<>();   // Target -> Indexnumber of column in real file where src data can be found

    private static Map<String, String> targetSources = new HashMap<>();

    private static Map<String, Map<String, Long>> longMappings = new TreeMap<>();

    public static void main(String[] args) {
        MainClass converter = new MainClass();
        CmdLineParser parser = new CmdLineParser(converter);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            System.exit(1);
        }
        converter.run();
    }

    private void run(){
        readHeaders();
        determineMappings();
        createMappings(tokenFile);
        File dataDir = new File(data);
        File[] dataFiles = dataDir.listFiles();
        CsvInputParser parser = new CsvInputParser(existingHeaderArray);
        CsvInputRecord record;
        int counter = 0;
            for (File file : dataFiles) {
                try {
                    boolean writeHeaders = true;
                    System.out.println("Started writing file:" + counter);
                    long startTime = System.currentTimeMillis();
                    File writeFile = new File(file.getName() + "_alt.csv");
                    FileWriter fwriter = new FileWriter(writeFile);
                    BufferedWriter writer = new BufferedWriter(fwriter);
                    parser.beginParsing(file);
                    while ((record = parser.parseNextRecord()) != null) {
                        boolean complete = false;
                        StringBuilder sb = new StringBuilder();
                        if(writeHeaders){
                            for(String header: existingHeaders){
                                if (existingHeaders.indexOf(header) != 0) {
                                    sb.append(",\"" + header + "\"");
                                } else {
                                    sb.append("\"" + header + "\"");
                                }
                            }
                            for(String target: targets){
                                sb.append(",\"" + target + "\"");
                            }
                            for(String header: newHeaders){
                                sb.append(",\"" + header + "\"");
                            }
                            sb.append("\n");
                            writer.write(sb.toString());
                            sb = new StringBuilder();
                            writeHeaders = false;
                        }
                        if(!fileHasHeaders) {
                            for (String header : existingHeaders) {
                                // Only temporary
                                String field = record.getString(header);
                                if (existingHeaders.indexOf(header) != 0) {
                                    if (field != null && field.startsWith("\""))
                                        sb.append("," + field);
                                    else if (field != null) {
                                        sb.append(",\"" + field + "\"");
                                    } else {
                                        sb.append(",\"\"");
                                    }
                                } else {
                                    if (field != null && field.startsWith("\""))
                                        sb.append(field);
                                    else if (field != null) {
                                        sb.append("\"" + field + "\"");
                                    } else {
                                        sb.append("\"\"");
                                    }
                                }
                            }
                            {
                                for (String target : targets) {
                                    if (longMappings.containsKey(target)) {
                                        Long key = longMappings.get(target).get(record.getString(targetSources.get(target)).toLowerCase());
                                        if (key != null) {
                                            sb.append(",\"" + key + "\"");
                                            complete = true;
                                        } else {
                                            sb.append(",\"\"");
                                        }
                                    } else {
                                        String key = mappings.get(target).get(record.getString(targetSources.get(target)).toLowerCase());
                                        if (key != null) {
                                            if (list) {
                                                key = key.replaceAll(";", Character.toString((char) 0x1e));
                                                sb.append(",\"" + key + "\"");
                                                complete = true;
                                            } else {
                                                sb.append(",\"" + key + "\"");
                                                complete = true;
                                            }
                                        } else {
                                            sb.append(",\"\"");
                                        }
                                    }
                                }
                                for(String value: newValues){
                                    sb.append(",\"" + value + "\"");
                                }
                            }
                            sb.append("\n");
                            if (incomplete.equals("true") && !complete) {

                            } else {
                                writer.write(sb.toString());
                            }
                        }
                        else{
                            fileHasHeaders = false;
                        }
                    }
                    counter += 1;
                    writer.close();
                    System.out.println("Finished writing file:" + file + "_alt.csv");
                    long endTime = System.currentTimeMillis();
                    System.out.println("Time: " + (endTime - startTime));
                }
                catch(IOException e){
                    log.error(e.getMessage());
                }
            }
        System.out.println("Finished");
    }

    private static void determineMappings(){
        if (!settings.equals("")) {
            String[] pieces = settings.split(",");
            int count = pieces.length;
            for (int i = 0; i < count; i = i + 2) {
                mapping.put(pieces[i + 1], existingHeaders.indexOf(pieces[i]));
                targetSources.put(pieces[i + 1], pieces[i]);
                targets.add(pieces[i + 1]);
            }
        }
        if(!newRows.equals("")){
            String[] parts = newRows.split(";");
            for(int i = 0; i < parts.length; i=i+2){
                newHeaders.add(parts[i]);
                newValues.add(parts[i+1]);
            }
        }
    }
    private static void createMappings(String file){
        File tokenFile = new File(file);
        if(file.endsWith(".csv")) {
            CsvParserSettings settings = new CsvParserSettings();
            settings.detectFormatAutomatically();
            settings.getFormat().setDelimiter('|');
            settings.setQuoteDetectionEnabled(true);
            settings.setHeaders(tokenHeaderArray);
            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(tokenFile);
            String[] record;
            parser.parseNext();
            longMappings.put("token", new TreeMap<>());
            System.out.println("Started mapping");
            while ((record = parser.parseNext()) != null) {
                for (String target : targets) {
                    if (target.equals("token")) {
                        longMappings.get(target).put(record[tokenHeaderList.indexOf(targetSources.get(target))], Long.parseLong(record[tokenHeaderList.indexOf(target)]));
                    } else {
                        mappings.get(target).put(record[tokenHeaderList.indexOf(targetSources.get(target))], record[tokenHeaderList.indexOf(target)]);
                    }
                }
            }
            parser.stopParsing();
            System.out.println("Finished mapping");
        }
        else if(file.endsWith(".parquet")){
            ParquetInputParser parser = new ParquetInputParser();
            parser.beginParsing(tokenFile);
            ParquetInputRecord record;
            parser.parseNextRecord();
            mappings.put("interests", new HashMap<>());
            System.out.println("Started mapping");
            while ((record = parser.parseNextRecord()) != null) {
                for (String target : targets) {
                    mappings.get(target).put(record.getString(targetSources.get(target)),record.getString(target));
                }
            }
            parser.stopParsing();
            System.out.println("Finished mapping");
        }
        else if(file.endsWith(".gz")){
            try {
                GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file));
                BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
                String record;
                mappings.put("interests", new HashMap<>());
                System.out.println("Started mapping");
                br.readLine();
                while((record = br.readLine()) != null){
                    String[] pieces = record.split(",");
                    for (String target : targets) {
                        mappings.get(target).put(pieces[tokenHeaderList.indexOf(targetSources.get(target))], pieces[tokenHeaderList.indexOf(target)]);
                    }
                }
            }
            catch(IOException e){
                log.error(e.getMessage());
            }
        }
    }

    private static void readHeaders(){
        if(isList.equals("true")){
            list = true;
        }
        if(!headers.equals("")) {
            existingHeaderArray = headers.split(",");
            existingHeaders = new ArrayList<>(Arrays.asList(headers.split(",")));
            fileHasHeaders = false;
        }
        else{
            File dataDir = new File(data);
            File[] dataFiles = dataDir.listFiles();
            CsvInputParser parser = new CsvInputParser();
            parser.beginParsing(dataFiles[0]);
            existingHeaderArray = parser.header;
            existingHeaders = new ArrayList<>(Arrays.asList(parser.header));
            needsHeaders = true;
            fileHasHeaders = true;
        }

        if (predefinedTokenHeaders.equals("")) {
            try (BufferedReader br = new BufferedReader(new FileReader(tokenFile))) {
                String headerString = br.readLine();
                String[] headers = headerString.split("\\|");
                tokenHeaderArray = headers;
                tokenHeaderList = new ArrayList<>(Arrays.asList(headers));
            } catch (FileNotFoundException e) {
                log.error("Couldn't find the specified token file");
            } catch (IOException e) {
                log.error("IO error: " + e.getLocalizedMessage());
            }
        } else {
            tokenHeaderArray = predefinedTokenHeaders.split(",");
            tokenHeaderList = new ArrayList<>(Arrays.asList(tokenHeaderArray));
        }
    }
}