import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Created by YOGA710 on 4/20/2017.
 */
public class IndexEventsES {
    static String[] gender = new String[] {"male", "female"};
    static String[] year = new String[] {"2015", "2016","2017"};
    static String[] search = new String[] {"gameofthrones", "mahabarath","captain","xmen","shatiman","srikrishna","ashwin","akshay","kumar","winterfell"};
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

            fileWriter = new FileWriter("C:\\Users\\YOGA710\\elastic\\events100k.csv");
int cid=0;
            int uid=0;
            int day=0;
            int month=0;


            for (int i=1;i<=10000;i++){

                cid=ramdomInt(1,1000);
                day=ramdomInt(1,30);
                month=ramdomInt(1,9);
                fileWriter.append(ramdomInt(1,100000)+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("contentView");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(cid+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("");
                fileWriter.append(COMMA_DELIMITER);
                if(day<10)
                fileWriter.append("0"+day+"/0"+month+"/"+getRandom(year));
                else
                    fileWriter.append(day+"/0"+month+"/"+getRandom(year));
                fileWriter.append(NEW_LINE_SEPARATOR);

            }


            for (int i=1;i<=2000;i++){

                cid=ramdomInt(1,1000);
                day=ramdomInt(1,30);
                month=ramdomInt(1,9);
                fileWriter.append(ramdomInt(1,100000)+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("contentPlay");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(cid+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("");
                fileWriter.append(COMMA_DELIMITER);
                if(day<10)
                    fileWriter.append("0"+day+"/0"+month+"/"+getRandom(year));
                else
                    fileWriter.append(day+"/0"+month+"/"+getRandom(year));
                fileWriter.append(NEW_LINE_SEPARATOR);

            }


            for (int i=1;i<=500;i++){

                cid=ramdomInt(1,1000);
                day=ramdomInt(1,30);
                month=ramdomInt(1,9);
                fileWriter.append(ramdomInt(1,100000)+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(getRandom(search));
                fileWriter.append(COMMA_DELIMITER);
                if(day<10)
                    fileWriter.append("0"+day+"/0"+month+"/"+getRandom(year));
                else
                    fileWriter.append(day+"/0"+month+"/"+getRandom(year));
                fileWriter.append(NEW_LINE_SEPARATOR);

            }
            for (int i=1;i<=20;i++){

                cid=ramdomInt(1500,5500);
                day=ramdomInt(1,30);
                month=ramdomInt(1,9);
                fileWriter.append(ramdomInt(1,100000)+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("purchase");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(cid+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("");
                fileWriter.append(COMMA_DELIMITER);
                if(day<10)
                    fileWriter.append("0"+day+"/0"+month+"/"+getRandom(year));
                else
                    fileWriter.append(day+"/0"+month+"/"+getRandom(year));
                fileWriter.append(NEW_LINE_SEPARATOR);

            }

            for (int i=1;i<=900;i++){

                cid=ramdomInt(10,50);
                day=ramdomInt(1,30);
                month=ramdomInt(1,9);
                fileWriter.append(ramdomInt(1,100000)+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("sessionDuration");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append(cid+"");
                fileWriter.append(COMMA_DELIMITER);
                fileWriter.append("");
                fileWriter.append(COMMA_DELIMITER);
                if(day<10)
                    fileWriter.append("0"+day+"/0"+month+"/"+getRandom(year));
                else
                    fileWriter.append(day+"/0"+month+"/"+getRandom(year));
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
