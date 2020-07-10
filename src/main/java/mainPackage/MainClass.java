package mainPackage;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPOutputStream;


@Slf4j
public class MainClass {


    @Option(name = "--data", required = true, usage = "Data folder")
    private static String data = "";

    @Option(name = "--tokenFile", required = true, usage = "Token file")
    private static String tokenFile = "";

    @Option(name = "--mappings", required = true, usage = "Header mappings. All mappings should be: source1,target1(new row); source2,target2.")
    private static String settings = "";

    @Option(name = "--existingHeaders", required = true, usage = "Existing headers.")
    private static String headers = "";

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
                    System.out.println("Started writing file:" + counter);
                    GZIPOutputStream zipWriter = new GZIPOutputStream(new FileOutputStream(new File(file.getName() + "_alt.gz")));
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(zipWriter, StandardCharsets.UTF_8));
                    parser.beginParsing(file);
                    while ((record = parser.parseNextRecord()) != null) {
                        StringBuilder sb = new StringBuilder();
                        for (String header : existingHeaders){
                            String field = record.getString(header);
                            if(existingHeaders.indexOf(header) != 0) {
                                if(field != null && field.startsWith("\""))
                                    sb.append(","+ field);
                                else
                                    sb.append(",\"" + field + "\"");
                            }
                            else{
                                if(field != null && field.startsWith("\""))
                                    sb.append(field);
                                else
                                    sb.append("\"" + field + "\"");
                            }
                        }
                        for (String target : targets) {
                            if (longMappings.containsKey(target)) {
                                Long key = longMappings.get(target).get(record.getString(targetSources.get(target)).toLowerCase());
                                if (key != null) {
                                    sb.append("," + key);
                                } else {
                                    sb.append(",\"\"");
                                }
                            } else {
                                String key = mappings.get(target).get(record.getString(targetSources.get(target)).toLowerCase());
                                if (key != null) {
                                    sb.append("," + key);
                                } else {
                                    sb.append(",\"\"");
                                }

                            }
                        }
                        sb.append("\n");
                        writer.write(sb.toString());
                    }
                    counter += 1;
                    writer.close();
                    zipWriter.finish();
                    System.out.println("Finished writing file:" + file + "_alt.gz");
                }
                catch(IOException e){
                    log.error(e.getMessage());
                }
            }
    }

    private static void determineMappings(){
        String[] pieces = settings.split(",");
        int count = pieces.length;
        for(int i = 0; i < count; i = i + 2){
            mapping.put(pieces[i+1], existingHeaders.indexOf(pieces[i]));
            targetSources.put(pieces[i+1], pieces[i]);
            targets.add(pieces[i+1]);
        }
    }
    private static void createMappings(String file){
        File tokenFile = new File(file);
        CsvParserSettings settings = new CsvParserSettings();
        settings.detectFormatAutomatically();
        settings.getFormat().setDelimiter('|');
        settings.setQuoteDetectionEnabled(true);
        settings.setHeaders(tokenHeaderArray);
        CsvParser parser = new CsvParser(settings);
        parser.beginParsing(tokenFile);
        String[] record;
        parser.parseNext();
        mappings.put("hasTv", new HashMap<>());
        longMappings.put("token", new HashMap<>());
        System.out.println("Started mapping");
        while ((record = parser.parseNext()) != null) {
            for(String target: targets){
                if(target.equals("token")){
                    longMappings.get(target).put(record[tokenHeaderList.indexOf(targetSources.get(target))],Long.parseLong(record[tokenHeaderList.indexOf(target)]));
                }
                else {
                    mappings.get(target).put(record[tokenHeaderList.indexOf(targetSources.get(target))], record[tokenHeaderList.indexOf(target)]);
                }
            }
        }
        parser.stopParsing();
        System.out.println("Finished mapping");
    }

    private static void readHeaders(){
        existingHeaderArray = headers.split(",");
        existingHeaders = new ArrayList<>(Arrays.asList(headers.split(",")));
        try(BufferedReader br = new BufferedReader(new FileReader(tokenFile))){
            String headerString = br.readLine();
            String[] headers = headerString.split("\\|");
            tokenHeaderArray = headers;
            tokenHeaderList = new ArrayList<>(Arrays.asList(headers));
        }
        catch(FileNotFoundException e){
            log.error("Couldn't find the specified token file");
        }
        catch(IOException e){
            log.error("IO error: " + e.getLocalizedMessage());
        }
    }
}