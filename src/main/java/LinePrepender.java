import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LinePrepender {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: java LinePrepender <file_path> <output_file_path>");
            System.exit(1);
        }

        String inPath = args[0], outPath = args[1];

        // If inPath is a file, interpret the outPath as the output path of the input file
        // Else, treat the outPath as extra data to add to the modified file name
        File inputPath = new File(inPath);

        if(inputPath.isDirectory()) {
            for(File file : inputPath.listFiles()) {
                if(!file.isDirectory()) {
                    prepend(file.getPath(), file.getPath() + "." + outPath);
                }
            }
        } else {
            prepend(args[0], args[1]);
        }
    }

    private static void prepend(String filePath, String outputPath) {

        try {
            BufferedWriter output = new BufferedWriter(new FileWriter(new File(outputPath)));
            BufferedReader input = new BufferedReader(new FileReader(new File(filePath)));

            int lineNum = 0;
            String line;
            while((line = input.readLine()) != null) {
                output.write(String.format("%d %s", ++lineNum, line));
                output.newLine();
            }

            input.close();
            output.close();

        }catch(FileNotFoundException e) {
            e.printStackTrace();
        }catch(IOException e) {
            e.printStackTrace();
        }
    }
}
