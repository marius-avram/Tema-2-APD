import java.util.ArrayList;

/** Interfata ce reprezinta o solutie partiala pentru problema de rezolvat. Aceste
 *  solutii partiale constituie task-uri care sunt introduse in workpool. In cazul 
 *  problemei noastre de indexare a documentelor avem 2 tipuri de solutii partiale/
 *  taskuri: map si reduce. Din aceasta cauza folosim o interfata cu metoda execute 
 *  ce va trebui implementata de ambele tipuri de taskuri. Astfel in Worker doar se 
 *  va apela metoda execute indiferent de tipul taskului.
 */
interface PartialSolution {
	
	public void execute();

}

/** Clasa ce reprezinta un thread worker. */
class Worker extends Thread {
	WorkPool wp;
	
	// Aici tinem rezultale finale. Primele X documente si frecvente
	// cuvintelor cheie asociate lor. 
	ArrayList<String> filesFound;
	ArrayList<float[]> frequencies;
	
	public Worker(WorkPool workpool) {
		this.wp = workpool;
		this.filesFound = new ArrayList<String>();
		this.frequencies = new ArrayList<float[]>();
	}

	/** Procesarea unei solutii partiale. Desi executarea taskurilor are
	 *  loc in mod identic indiferent de clasa ce implementeaza PartialSolution, 
	 *  trebuie efectuate si cateva operatii specifice fiecarui tip de task.*/
	void processPartialSolution(PartialSolution ps) {
		String filename;
		ps.execute();
		if (ps instanceof MapTask){
			MapTask task = (MapTask)ps;
			// Se adauga cuvintele in ConcurrentHashMap-ul din Main
			filename = task.getFilename();
			Main.addWordsToMap(filename, task.getWords());
			
		}
		else if(ps instanceof ReduceTask){
			ReduceTask task = (ReduceTask)ps;
			if (task.containsKeywords()){
				// Se adauga la lista fisiere care indeplineste conditia 
				// impusa de ce-a dea doua operatie de tip reduce
				filesFound.add(task.getFilename());
				frequencies.add(task.getKeywordsFrequencies());
			}
		}
	}
	
	
	/** Returneaza lista de fisiere care se gaseste printre rezultatele finale */
	public ArrayList<String> getFiles(){
		return filesFound;
	}
	
	/** Returneaza frecventele cuvintelor in fisierele ce sunt rezultate finale */
	public ArrayList<float[]> getFrequencies(){
		return frequencies;
	}
	
	
	public void run() {
		// System.out.println("Thread-ul worker " + this.getName() + " a pornit...");
		while (true) {
			PartialSolution ps = wp.getWork();
			if (ps == null)
				break;
			
			processPartialSolution(ps);
		}
		// System.out.println("Thread-ul worker " + this.getName() + " s-a terminat...");
	}

	
}



