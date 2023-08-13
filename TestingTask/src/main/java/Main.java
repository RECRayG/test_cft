public class Main {
    public static void main(String[] args) {
        try {
            FileJob fileJob = new FileJob(args);
            fileJob.mergeSortFiles();
            System.out.println("Успешное слияние файлов!");
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
        }
    }
}