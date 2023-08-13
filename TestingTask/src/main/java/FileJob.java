import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class FileJob {
    // Вспомогательный класс для хранения элементов и их позиций
    private static class Element<T> {
        T value;
        int arrayIndex;

        public Element(T value, int arrayIndex) {
            this.value = value;
            this.arrayIndex = arrayIndex;
        }
    }
    //// Хранение параметров командной строки:
    // Режим сортиовки
    private String sortMode;
    // Тип данных
    private String dataType;
    // Имя выходного файла
    private String outFileName;
    // Имена входных файлов
    private String[] inFilesName;
    // Имена временных файлов
    private String[] inFilesNameBuffer;

    // Максимальное кол-во строк в файле, при превышении которого идёт его разбиение на чанки
    public static final int FILE_BUFFER_SIZE = 10000;

    // Пользовательская директория
    public static final String directory = System.getProperty("user.dir") + "\\";

    // Константы (возможные параметры командной строки) для работы алгоритмов
    public static final String SORT_MODE_INCREASE = "-a";
    public static final String SORT_MODE_DECREASE = "-d";

    public static final String DATA_TYPE_INTEGER = "-i";
    public static final String DATA_TYPE_STRING = "-s";

    public FileJob(String[] args) throws RuntimeException {
        List<String> temp = Arrays.asList(args);

        //// Логика установки параметров
        // Режим сортировки (возрастание/убывание)
        if(temp.contains(SORT_MODE_INCREASE) && temp.contains(SORT_MODE_DECREASE)) {
            sortMode = SORT_MODE_DECREASE;
        } else {
            if(temp.contains(SORT_MODE_INCREASE)) {
                sortMode = SORT_MODE_INCREASE;
            } else if(temp.contains(SORT_MODE_DECREASE)) {
                sortMode = SORT_MODE_DECREASE;
            } else {
                sortMode = SORT_MODE_INCREASE;
            }
        }

        // Тип данных (строковый/целочисленный)
        if(temp.contains(DATA_TYPE_INTEGER) && temp.contains(DATA_TYPE_STRING)) {
            if(temp.indexOf(DATA_TYPE_INTEGER) < temp.indexOf(DATA_TYPE_STRING)) {
                dataType = DATA_TYPE_INTEGER;
            } else {
                dataType = DATA_TYPE_STRING;
            }
        } else {
            if(temp.contains(DATA_TYPE_INTEGER)) {
                dataType = DATA_TYPE_INTEGER;
            } else if(temp.contains(DATA_TYPE_STRING)) {
                dataType = DATA_TYPE_STRING;
            }
        }

        // Получение имён файлов
        List<String> tempFilesName = temp.stream().filter(fileName ->
                                                            fileName.contains(".")
                                                        ).collect(Collectors.toList());

        // Имена файлов (входные/выходные)
        if(tempFilesName.size() >= 2) {
            outFileName = tempFilesName.get(0);
            tempFilesName.remove(0);
            List<String> tempFilesNameSorted = new ArrayList<>();

            // Проверка на сортированность файлов
            for(int i = 0; i < tempFilesName.size(); i++) {
                if(isSorted(tempFilesName.get(i))) {
                    tempFilesNameSorted.add(tempFilesName.get(i));
                }
            }

            if(tempFilesNameSorted.isEmpty())
                throw new RuntimeException("Ни один из файлов не отсортирован");

            inFilesName = tempFilesNameSorted.stream().toArray(String[]::new);
            inFilesNameBuffer = new String[0];
        } else {
            System.out.println("Вы не ввели минимальное колчество названий файлов: 2");
            return;
        }

        // Проверить файл на сортировку и запомнить состояние
        checkSortState();
    }

    private void checkSortState() throws RuntimeException {
        Arrays.asList(inFilesName).stream().forEach(fileName -> {
            // Проверить файл на сортировку и запомнить состояние
            isSorted(fileName);
        });
    }

    public void mergeSortFiles() {
        // Использовать сортировку, в зависимости от параметра
        // (не исползуются дженерики, чтобы был смысл у параметров командной строки)
        switch(dataType) {
            case DATA_TYPE_INTEGER:
                try{
                    externalIntMergeSort(Arrays.asList(inFilesName), directory + outFileName);
                } catch(IOException e) {
                    System.err.println("Непредвиденная ошибка чтения/записи файла: " + e.getMessage());
                }
                break;
            case DATA_TYPE_STRING:
                try{
                    externalStrMergeSort(Arrays.asList(inFilesName), directory + outFileName);
                } catch(IOException e) {
                    System.err.println("Непредвиденная ошибка чтения/записи файла: " + e.getMessage());
                }
                break;
        }

        // Удаление временных файлов
        Arrays.asList(inFilesNameBuffer).stream().forEach(file -> {
            deleteFile(file);
        });
    }

    private void externalIntMergeSort(List<String> inputFiles, String outputFile) throws IOException {
        // Разбиваем каждый входной файл, если кол-во его строк больше заданного значения
        List<String> chunks = new ArrayList<>();
        for (String inputFile : inputFiles) {
            List<String> chunkFiles = splitIntChunks(inputFile, FILE_BUFFER_SIZE);
            // Запомнить названия временных файлов
            inFilesNameBuffer = Stream.concat(Arrays.asList(inFilesNameBuffer).stream(), chunkFiles.stream())
                                        .toArray(String[]::new);

            chunks.addAll(chunkFiles);
        }

        // Объединяем чанки с помощью сортировки слиянием
        List<Integer> mergedData = mergeSortedIntChunks(chunks);

        // Записываем объединенные отсортированные данные обратно в выходной файл
        writeIntDataToFile(mergedData, outputFile);
    }

    private void externalStrMergeSort(List<String> inputFiles, String outputFile) throws IOException {
        // Разбиваем каждый входной файл, если кол-во его строк больше заданного значения
        List<String> chunks = new ArrayList<>();
        for (String inputFile : inputFiles) {
            List<String> chunkFiles = splitStrChunks(inputFile, FILE_BUFFER_SIZE);
            // Запомнить названия временных файлов
            inFilesNameBuffer = Stream.concat(Arrays.asList(inFilesNameBuffer).stream(), chunkFiles.stream())
                    .toArray(String[]::new);

            chunks.addAll(chunkFiles);
        }

        // Объединяем чанки с помощью сортировки слиянием
        List<String> mergedData = mergeSortedStrChunks(chunks);

        // Записываем объединенные отсортированные данные обратно в выходной файл
        writeStrDataToFile(mergedData, outputFile);
    }

    private List<String> splitIntChunks(String inputFile, int chunkSize) throws IOException {
        List<String> chunkFiles = new ArrayList<>();
        List<Integer> chunkData = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                chunkData.add(Integer.parseInt(line));
                count++;
                if (count == chunkSize) {
                    // Если сортировка в порядке убывания, то перевернуть данные в файла
                    if(sortMode.equals(SORT_MODE_DECREASE))
                        reverseIntSort(chunkData);

                    String chunkFileName = "chunk_" + UUID.randomUUID().toString() + ".txt";
                    writeIntDataToFile(chunkData, chunkFileName);
                    chunkFiles.add(chunkFileName);
                    chunkData.clear();
                    count = 0;
                }
            }
        }

        // Если данные ещё остались
        if (!chunkData.isEmpty()) {
            // Если сортировка в порядке убывания, то перевернуть данные в файла
            if(sortMode.equals(SORT_MODE_DECREASE))
                reverseIntSort(chunkData);

            String chunkFileName = "chunk_" + UUID.randomUUID().toString() + ".txt";
            writeIntDataToFile(chunkData, chunkFileName);
            chunkFiles.add(chunkFileName);
        }

        return chunkFiles;
    }

    private List<String> splitStrChunks(String inputFile, int chunkSize) throws IOException {
        List<String> chunkFiles = new ArrayList<>();
        List<String> chunkData = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                chunkData.add(line);
                count++;
                if (count == chunkSize) {
                    // Если сортировка в порядке убывания, то перевернуть данные в файла
                    if(sortMode.equals(SORT_MODE_DECREASE))
                        reverseStrSort(chunkData);

                    String chunkFileName = "chunk_" + UUID.randomUUID().toString() + ".txt";
                    writeStrDataToFile(chunkData, chunkFileName);
                    chunkFiles.add(chunkFileName);
                    chunkData.clear();
                    count = 0;
                }
            }
        }

        // Если данные ещё остались
        if (!chunkData.isEmpty()) {
            // Если сортировка в порядке убывания, то перевернуть данные в файла
            if(sortMode.equals(SORT_MODE_DECREASE))
                reverseStrSort(chunkData);

            String chunkFileName = "chunk_" + UUID.randomUUID().toString() + ".txt";
            writeStrDataToFile(chunkData, chunkFileName);
            chunkFiles.add(chunkFileName);
        }

        return chunkFiles;
    }

    private void reverseIntSort(List<Integer> list) {
        Collections.reverse(list);
    }

    private void reverseStrSort(List<String> list) {
        Collections.reverse(list);
    }

    private List<Integer> mergeSortedIntChunks(List<String> chunkFiles) {
        List<Integer> merged = new ArrayList<>();
        List<BufferedReader> readers = new ArrayList<>();

        for (String chunkFile : chunkFiles) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(chunkFile));
                readers.add(reader);
            } catch (IOException e) {
                System.err.println("Ошибка открытия файла: " + chunkFile + '\n' + e.getMessage());
            }
        }

        // Используем очередь с приоритетами, в зависимости от типа сортировки
        PriorityQueue<Element<Integer>> minHeap = null;
        switch(sortMode) {
            case SORT_MODE_INCREASE:
                minHeap = new PriorityQueue<>(comparatorIntNormal);
                break;
            case SORT_MODE_DECREASE:
                minHeap = new PriorityQueue<>(comparatorIntReverse);
                break;
        }

        // Инициализируем minHeap начальными элементами из каждого чанка
        for (int i = 0; i < readers.size(); i++) {
            try {
                String line = readers.get(i).readLine();
                if (line != null) {
                    int value = Integer.parseInt(line);
                    minHeap.offer(new Element<Integer>(value, i));
                }
            } catch (IOException e) {
                System.err.println("Ошибка чтения файла: " + e.getMessage());
            }
        }

        // Выполняем слияние отсортированных данных из разных файлов
        while (!minHeap.isEmpty()) {
            Element<Integer> min = minHeap.poll();
            merged.add(min.value);

            try {
                String line = readers.get(min.arrayIndex).readLine();
                if (line != null) {
                    int value = Integer.parseInt(line);
                    minHeap.offer(new Element<Integer>(value, min.arrayIndex));
                }
            } catch (IOException e) {
                System.err.println("Ошибка чтения файла: " + e.getMessage());
            }
        }

        // Закрываем всех читателей
        for (BufferedReader reader : readers) {
            try {
                reader.close();
            } catch (IOException e) {
                System.err.println("Ошибка закрытия файла: " + e.getMessage());
            }
        }

        return merged;
    }

    private List<String> mergeSortedStrChunks(List<String> chunkFiles) {
        List<String> merged = new ArrayList<>();
        List<BufferedReader> readers = new ArrayList<>();

        for (String chunkFile : chunkFiles) {
            try {
                BufferedReader reader = new BufferedReader(new FileReader(chunkFile));
                readers.add(reader);
            } catch (IOException e) {
                System.err.println("Ошибка открытия файла: " + chunkFile + '\n' + e.getMessage());
            }
        }

        // Используем очередь с приоритетами, в зависимости от типа сортировки
        PriorityQueue<Element<String>> minHeap = null;
        switch(sortMode) {
            case SORT_MODE_INCREASE:
                minHeap = new PriorityQueue<>(comparatorStrNormal);
                break;
            case SORT_MODE_DECREASE:
                minHeap = new PriorityQueue<>(comparatorStrReverse);
                break;
        }

        // Инициализируем minHeap начальными элементами из каждого чанка
        for (int i = 0; i < readers.size(); i++) {
            try {
                String line = readers.get(i).readLine();
                if (line != null) {
                    minHeap.offer(new Element<String>(line, i));
                }
            } catch (IOException e) {
                System.err.println("Ошибка чтения файла: " + e.getMessage());
            }
        }

        // Выполняем слияние отсортированных данных из разных файлов
        while (!minHeap.isEmpty()) {
            Element<String> min = minHeap.poll();
            // Если в строке есть пробелы, то пропускаемм невалидную строку, согласно ТЗ
            if(!min.value.contains(" ")) {
                merged.add(min.value);
            }

            try {
                String line = readers.get(min.arrayIndex).readLine();
                if (line != null) {
                    minHeap.offer(new Element<String>(line, min.arrayIndex));
                }
            } catch (IOException e) {
                System.err.println("Ошибка чтения файла: " + e.getMessage());
            }
        }

        // Закрываем всех читателей
        for (BufferedReader reader : readers) {
            try {
                reader.close();
            } catch (IOException e) {
                System.err.println("Ошибка закрытия файла: " + e.getMessage());
            }
        }

        return merged;
    }

    // Метод записи данных в файл
    private void writeIntDataToFile(List<Integer> data, String fileName) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (Integer value : data) {
                writer.write(value + "\n");
            }
        }
    }

    // Метод записи данных в файл
    private void writeStrDataToFile(List<String> data, String fileName) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            for (String value : data) {
                writer.write(value + "\n");
            }
        }
    }

    // Анонимный класс для сравнения целых чисел в порядке возрастания
    public static Comparator<Element<Integer>> comparatorIntNormal = new Comparator<Element<Integer>>() {
        @Override
        public int compare(Element<Integer> o1, Element<Integer> o2) {
            if(o1.value > o2.value)
                return 1;
            else if(o1.value < o2.value)
                return -1;
            else
                return 0;
        }
    };

    // Анонимный класс для сравнения целых чисел в порядке убывания
    public static Comparator<Element<Integer>> comparatorIntReverse = new Comparator<Element<Integer>>() {
        @Override
        public int compare(Element<Integer> o1, Element<Integer> o2) {
            if(o1.value > o2.value)
                return -1;
            else if(o1.value < o2.value)
                return 1;
            else
                return 0;
        }
    };

    // Анонимный класс для сравнения строк в порядке возрастания
    public static Comparator<Element<String>> comparatorStrNormal = new Comparator<Element<String>>() {
        @Override
        public int compare(Element<String> o1, Element<String> o2) {
            if(o1.value.toLowerCase().compareTo(o2.value.toLowerCase()) > 0)
                return 1;
            else if(o1.value.toLowerCase().compareTo(o2.value.toLowerCase()) < 0)
                return -1;
            else
                return 0;
        }
    };

    // Анонимный класс для сравнения строк в порядке убывания
    public static Comparator<Element<String>> comparatorStrReverse = new Comparator<Element<String>>() {
        @Override
        public int compare(Element<String> o1, Element<String> o2) {
            if(o1.value.toLowerCase().compareTo(o2.value.toLowerCase()) > 0)
                return -1;
            else if(o1.value.toLowerCase().compareTo(o2.value.toLowerCase()) < 0)
                return 1;
            else
                return 0;
        }
    };

    // Метод, проверяющий тип данных в файле
    private boolean checkType(List<String> block) {
        boolean isIntegerBlock = true;
        for (String line : block) {
            try {
                Integer.parseInt(line);
            } catch (NumberFormatException e) {
                isIntegerBlock = false;
                break;
            }
        }

        return isIntegerBlock;
    }

    // Метод, проверяющий отсортированность данных в файле
    private boolean isSorted(String fileName) throws RuntimeException {
        boolean isSorted = true;

        try (BufferedReader br = new BufferedReader(new FileReader(directory + fileName))) {
            List<String> block = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                block.add(line.replace(" ", ""));
            }

            boolean isInteger = checkType(block);
            if((dataType.equals(DATA_TYPE_STRING) && isInteger) ||
                (dataType.equals(DATA_TYPE_INTEGER) && !isInteger))
            {
                throw new RuntimeException("Вы ввели параметр, не соответствующий типу обрабатываемого файла: " + fileName);
            }

            if(isInteger) {
                isSorted = isSortedInt(block.stream().map(Integer::parseInt).collect(Collectors.toList()));
            } else {
                isSorted = isSortedStr(block, block.size());
            }

        } catch (IOException e) {
            System.out.println("Ошибка чтения файла: " + fileName);
        }

        return isSorted;
    }

    // Вспомогателный метод, проверяющий отсортированность списка целых чисел
    private boolean isSortedInt(List<Integer> block) {
        if (block == null || block.size() <= 1) {
            return true;
        }

        return IntStream.range(0, block.size() - 1).noneMatch(i -> block.get(i) > block.get(i + 1));
    }

    // Вспомогателный метод, проверяющий отсортированность списка строк
    public static boolean isSortedStr(List<String> listOfStrings, int index) {
        if (index < 2) {
            return true;
        } else if (listOfStrings.get(index - 2).trim().compareTo(listOfStrings.get(index - 1).trim()) > 0) {
            return false;
        } else {
            return isSortedStr(listOfStrings, index - 1);
        }
    }

    private void deleteFile(String fileName) {
        File fileToDelete = new File(directory + fileName);

        // Проверяем, существует ли файл
        if (fileToDelete.exists()) {
            // Пытаемся удалить файл
            if (fileToDelete.delete()) {
                System.out.println("Файл " + fileName + " успешно удален.");
            } else {
                System.out.println("Не удалось удалить файл " + fileName + ".");
            }
        } else {
            System.out.println("Файл " + fileName + " не существует.");
        }
    }
}
