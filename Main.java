import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Vector;
import java.util.HashMap;

public class Main {
	
	static int numWords, dimFragment, numMostFrequent, numAswers, numFiles;
	// Aici se tin cuvintele asociate fiecarui fisier. Cuvintele sunt 
	// obtinute in urma efectuarii operatiei de tip map. Cheia este 
	// numele fisierului iar vectorul contine cuvintele corespunzatoare
	// unui anumit fragment impreuna cu frecventele asociate acestuia.
	// Am folosit Vector pentru ca acesta este thread-safe.
	static HashMap<String, Vector<HashMap<String, Integer>>> map = null;
	
	// Returneaza dimensiunea unui fisier in octeti
	// Numarul de octeti = numarul de caractere
	public static long getFileSize(String name){
		File file = new File(name);
		if (!file.exists() || !file.isFile()){
			return 0;
		}
		return file.length();
	}
	
	// Returneaza limitele fragmentelor. De exemplu pentru un fragment de
	// dimensiune 500 lista va contine [0,499, 998, 1497, etc..]
	public static ArrayList<Long> createFragments(long size, int dimFragment){
		ArrayList<Long> fragmentsLimits = new ArrayList<Long>();
		for(long i=0; i<size; i+=(dimFragment-1)){
			fragmentsLimits.add(i);
		}
		// La sfarsit adauga si limita fragmentului de la sfarsitul fisierului.
		if(fragmentsLimits.get(fragmentsLimits.size()-1)!=size-1){
			fragmentsLimits.add(size-1);
		}
		return fragmentsLimits;
	}
	
	// Adauga in map o perechile de tip (cheie, valoare) ce corespund cuvintelor 
	// si frecventelor acestora.
	public static void addWordsToMap(String filename, HashMap<String, Integer> words){
			map.get(filename).add(words);
	}
	
	// Metoda main
	public static void main(String[] args){

		int numThreads = 0;
		String fileIn = "", fileOut = "";
		String line;
		String keywords[], files[];
		
		// Prelucram argumentele date in linia de comanda
		if (args.length != 3){
			System.out.println("Programul trebuie rulat cu argumetele NT fisin fisout.");
			System.exit(1);
		}
		else {
			try{
				numThreads = Integer.parseInt(args[0]);
			} catch (NumberFormatException e){
				System.out.println("Primul argument trebuie sa fie un intreg.");
				System.exit(1);
			}
				fileIn = args[1];
				fileOut = args[2];
		}
		
		
		try {
			FileInputStream fileInStream = new FileInputStream(fileIn);
			BufferedReader fileInReader = new BufferedReader(new InputStreamReader(fileInStream));
			// Citirea numarului de cuvinte cheie cautate
			line = fileInReader.readLine();
			numWords = Integer.parseInt(line);
			
			// Citirea numWords cuvinte cheie cautate 
			line = fileInReader.readLine();
			keywords = line.split(" ");
			
			// Citirea dimensiunii fragmentelor
			line = fileInReader.readLine();
			dimFragment = Integer.parseInt(line);
			
			// Citirea numarului celor mai frecvente cuvinte retinute pentru fiecare document
			line = fileInReader.readLine();
			numMostFrequent = Integer.parseInt(line);
			
			// Citirea numarului de raspunsuri dorite
			line = fileInReader.readLine();
			numAswers = Integer.parseInt(line);
			
			// Citirea numarului de fisiere in care se va face cautarea
			line = fileInReader.readLine();
			numFiles = Integer.parseInt(line);
			
			// Citirea numelor fisierelor
			files = new String[numFiles];
			for(int i=0; i<numFiles; i++){
				files[i] = fileInReader.readLine();
			}
			
			fileInReader.close();
			
			
			
			MapTask t; 
			// Creeam un workpool si introducem datele in workpool
			WorkPool workpool = new WorkPool(numThreads);
			// De asemnea se intializeaza map-ul ce urmeaza sa fie folosit de toate threadurile
			map = new HashMap<String, Vector<HashMap<String, Integer>>>();
			for(int i=0; i<numFiles; i++){
				ArrayList<Long> fragments = createFragments(getFileSize(files[i]), dimFragment);
				map.put(files[i], new Vector<HashMap<String, Integer>>());
				for(int j=0; j<fragments.size()-1; j++){
					// Afisarea fragmentelor 
					t = new MapTask(files[i], fragments.get(j), fragments.get(j+1));
					workpool.putWork(t);
				}
			}
			
			
			// Creeam un numar de workeri egal nu cu numarul de threaduri
			Worker[] workersMap = new Worker[numThreads];
			for(int i=0; i<numThreads; i++){
				workersMap[i] = new Worker(workpool);
			}
			
			// Pornim workeri
			for(int i=0; i<numThreads; i++){
				workersMap[i].start();
			}
			
			// Facem join
			try{
				for(int i=0; i<numThreads; i++){
					workersMap[i].join();
				}
			} catch(Exception e){
				System.out.println("Exceptie la join.");
			}
			
			// Se ajunge la etapa intermediara unde trebuie din nou 
			// asignate niste taskuri unui workpool. De acesta data 
			// taskurile sunt de tip Reduce
			for(int i=0; i<numFiles; i++){
				ReduceTask task = new ReduceTask(files[i], map.get(files[i]), keywords);
				workpool.putWork(task);
			}
			
			
			
			// Cream noi workeri
			Worker[] workersReduce= new Worker[numThreads];
			for(int i=0; i<numThreads; i++){
				workersReduce[i] = new Worker(workpool);
			}
			
			// Apelam functia start pentru ai pune sa proceseze taskurile
			for(int i=0; i<numThreads; i++){
				workersReduce[i].start();
			}
			
			// Facem din nou join
			try{
				for(int i=0; i<numThreads; i++){
					workersReduce[i].join();
				}
			} catch(Exception e) {
				System.out.println("Exceptie la join");
			}
			
			// Luam rezultatele din toti workerii
			ArrayList<String> filesFound = new ArrayList<String>();
			ArrayList<float[]> frequencies = new ArrayList<float[]>();
			
			for(int i=0; i<numThreads; i++){
				filesFound.addAll(workersReduce[i].getFiles());
				frequencies.addAll(workersReduce[i].getFrequencies());
			}
			
			// Facem afisarea rezultatelor in fisierul de iesire 
			FileWriter fstreamOut = new FileWriter(fileOut);
			BufferedWriter fileOutWriter = new BufferedWriter(fstreamOut);
			int answersGiven = 0, index = -1;
			
			fileOutWriter.write("Rezultate pentru: (");
			for(int i=0; i<numWords; i++){
				fileOutWriter.write(keywords[i]); 
				if (i+1!=numWords){
					fileOutWriter.write(", ");
				}
			}
			fileOutWriter.write(")");
			fileOutWriter.newLine();
			fileOutWriter.newLine();
			
			for(int i=0; i<numFiles; i++){
				index = filesFound.indexOf(files[i]);
				if (index != -1){
					// Afisam numele fisierului
					fileOutWriter.write(files[i] + " (");
					// Si frecventele corespunzatoare cuvintelor cheie
					for(int j=0; j<frequencies.get(index).length; j++){
						// Afisam floatul cu doar 2 zecimale
						BigDecimal bdFrequency = new BigDecimal(frequencies.get(index)[j]);
						bdFrequency = bdFrequency.setScale(2, RoundingMode.FLOOR);
						fileOutWriter.write(bdFrequency + "");
						//System.out.println(bdFrequency + " " + frequencies.get(index)[j]);
						if (j+1!=frequencies.get(index).length){
							fileOutWriter.write(", ");
						}
					}
					fileOutWriter.write(")");
					fileOutWriter.newLine();
					answersGiven++;
				}
				if (answersGiven == numAswers){
					break;
				}
			}
			
			fileOutWriter.close();
			
			
			
		} catch (FileNotFoundException e){
			System.out.println("Fisierul " + fileIn + " nu exista.");
		} catch (IOException e){
			System.out.println("Eroare la citire din fisierul " + fileIn + ".");
		}
		
		System.out.println("Rezultatele au fost scrise in " + fileOut + ".");
		
		
		
		
	}
}
