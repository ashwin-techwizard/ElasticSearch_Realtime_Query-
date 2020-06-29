import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Created by YOGA710 on 4/20/2017.
 */
public class IndexES {
    static String[] gender = new String[] {"male", "female"};
    static String[] status = new String[] {"subscribed", "anonymous","registered"};
    static String[] location = new String[] {"India", "USA","UK","Denmark","Canada","Japan","Russia","China","Germany","France"};
    public static String getRandom(String[] array) {
        int rnd = new Random().nextInt(array.length);
        return array[rnd];
    }

    static int ramdomInt(int low,int high){
            Random r = new Random();
    int Result = r.nextInt(high-low) + low;
    return Result;
    }

    static String getAgeGroup(int age) {

        String ageGroup = "";
        if (age < 25) {
            ageGroup = "18-25";
        } else if (age < 30) {
            ageGroup = "25-30";
        } else if (age < 40) {
            ageGroup = "30-40";
        } else if (age < 60) {
            ageGroup = "40-60";
        }
        return ageGroup;
    }

    private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";

    public static void main(String[] args) {
        FileWriter fileWriter = null;

        try {

            fileWriter = new FileWriter("C:\\Users\\YOGA710\\elastic\\user100k.csv");
int age=0;
            for (int i=1;i<=1000001;i++){
                age=ramdomInt(18,60);
                fileWriter.append(i+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(getAgeGroup(age));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(age+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(getRandom(location));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(getRandom(gender));
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(getRandom(status));
                fileWriter.append(NEW_LINE_SEPARATOR);

            }



        }catch (Exception e) {

            System.out.println("Error in CsvFileWriter !!!");

            e.printStackTrace();

        }finally {

            try {

                fileWriter.flush();

                fileWriter.close();

            } catch (IOException e) {
                System.out.println("Error while flushing/closing fileWriter !!!");
                e.printStackTrace();
            }
        }

    }
}
