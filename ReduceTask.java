import java.util.*;


public class ReduceTask implements PartialSolution{
	private String filename; 
	private Vector<HashMap<String, Integer>> list;
	private HashMap<String, Integer> frequencies;
	private List<Map.Entry<String, Integer>> sortedFrequencies;
	private List<String> mostFrequentWords;
	private List<Float> mostFrequentFrequencies;
	private int numMostFrequent, numWords;
	private String keywords[];
	private boolean containsKeywords;
	private float keywordsFrequencies[];
	
	public ReduceTask(String filename, Vector<HashMap<String, Integer>> list, String keywords[]){
		this.filename = filename;
		this.list = list;
		this.numMostFrequent = Main.numMostFrequent;
		this.numWords = Main.numWords;
		this.keywords = keywords;
		// Frecventele asociate cuvintelor cheie se vor gasi in aceeasi 
		// ordine in care se gasesc in fisierul de intrare si in keywords 
		keywordsFrequencies = new float[numWords];
		frequencies = new HashMap<String, Integer>();
		mostFrequentWords = new ArrayList<String>();
		mostFrequentFrequencies = new ArrayList<Float>();
		containsKeywords = false;
	}
	
	@Override
	public void execute() {
		reduceFirstOperation();
		reduceSecondOperation();
	}
	/** Realizeaza prima operatie Reduce in care se aduna numarul aparitiilor 
	 *  fiecarui cuvant in text si se pastreaza cele mai frecvente cuvinte. 
	 *  Pentru o numarare eficienta am folosit un hashmap. */
	public void reduceFirstOperation(){
		
		int nth_frequency = 0;
		int totalNumWords = 0;
		// Interam pe intreaga lista de cuvinte
		// si adunam frecventele cuvintelor
		for (HashMap<String, Integer> map : list){
			for(Map.Entry<String, Integer> entry : map.entrySet()) {
				int frequency = entry.getValue();
				totalNumWords += frequency;
				if (frequencies.containsKey(entry.getKey())){
					frequency += frequencies.get(entry.getKey());
				}
				frequencies.put(entry.getKey(), frequency);
			}
		}
		
		// Sortam cuvintele in ordine descrescatoare dupa frecventa
		sortedFrequencies = new ArrayList<Map.Entry<String, Integer>>();
		for(Map.Entry<String, Integer> entry : frequencies.entrySet()){
			sortedFrequencies.add(entry);
		}
		Collections.sort(sortedFrequencies, new ComparatorFrecventa());
		
		
		
		float frequency = 0.0f;
		// Se pastreaza primele N cuvinte cu frecventele cele mai ridicate
		for (int i=0; i<sortedFrequencies.size(); i++){
			mostFrequentWords.add(sortedFrequencies.get(i).getKey());
			// Adaugam si frecventele calculate dupa formula 
			// (nr_aparitii/nr_total_cuvinte)*100
			frequency = (float)sortedFrequencies.get(i).getValue()/totalNumWords*100;
			mostFrequentFrequencies.add(frequency);
			if (i==numMostFrequent-1){
				nth_frequency = sortedFrequencies.get(i).getValue();
				break;
			}
		}
		
		// Trebuie sa adaugam toate cuvintele care au aceeasi frecventa cu 
		// frecventa ultimului cuvant adaugat in lista de cuvinte.
		for (int i=numMostFrequent; i<sortedFrequencies.size(); i++){
			if (sortedFrequencies.get(i).getValue() == nth_frequency){
				mostFrequentWords.add(sortedFrequencies.get(i).getKey());
				mostFrequentFrequencies.add(frequency);
			}
		}
		
			
	}
	
	/** Realizeaza a doua operatie reduce. Determina daca in fisierul corespunzator 
	 *  acestui task de tip reduce, toate cuvintele cheie se regasesc printre primele 
	 *  N cuvinte cele mai frecvente. */ 
	public void reduceSecondOperation(){
		boolean found;
		for(int i=0; i<numWords; i++){
			found = false;
			int j=0;
			for(String word : mostFrequentWords){
				if (keywords[i].compareTo(word)==0){
					// Retinem frecventa asociata cuvantului cheie
					keywordsFrequencies[i] = mostFrequentFrequencies.get(j);
					found = true;
					break;
				}
				j++;
			}
			if (!found) {
				containsKeywords = false;
				return;
			}
			
		}
		// Daca nu s-a iesit din functie pana acum
		// inseamna ca toate cuvintele cheie se gasesc in fisier
		containsKeywords = true;
	}

	public boolean containsKeywords() {
		return containsKeywords;
	}

	public String getFilename() {
		return filename;
	}

	public float[] getKeywordsFrequencies() {
		return keywordsFrequencies;
	}
	
	
}

class ComparatorFrecventa implements Comparator<Map.Entry<String, Integer>>{
	
	@Override
	public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
		return -a.getValue().compareTo(b.getValue());
	}
	
}
