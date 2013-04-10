import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

public class MapTask implements PartialSolution {
	private String filename;
	private long offsetStart, offsetEnd, end;
	private int size;
	private HashMap<String, Integer> words;
	private byte sequence[];
	
	public MapTask(String filename, long offsetStart, long offsetEnd){
		this.filename = filename; 
		this.offsetStart = offsetStart;
		this.offsetEnd = offsetEnd;
		this.size = (int)(long)offsetEnd-(int)(long)offsetStart;
		sequence = new byte[size];
		words = new HashMap<String, Integer>();
		
		// calculam offsetul sfarsitului de fisier
		end = Main.getFileSize(filename);
	}
	
	@Override
	/** Se realizeaza direct aici operatia de tip map care citeste o anumita portiune 
	 *  D din fisier si determina toate cuvintele din acea portiune. Citirea din fisier
	 *  se face in "paralel" cu ajutorul claseo RandomAccessFile.
	 */
	public void execute() {
		byte character;
		int pos = 0;
		String word = new String("");
		try{
			RandomAccessFile file = new RandomAccessFile(filename, "r");
			
			// Citim secventa ca pe un sir de bytes
			file.seek(offsetStart);
			file.read(sequence);
			

			// Cu exceptia primului fragment dintr-un fisier toate celelalte 
			// trebuie sa ignore primele caractere pana intalnesc un delimitator
			// (pentru ca in toate fragmentele cu exceptia primului aceste 
			// caractere au fost include intr-un cuvant din fragmentul anterior)
			if(offsetStart!=0){
				while(true){
					if (isSeparator(sequence[pos]) && !isSeparator(sequence[pos+1])){
						pos++;
						break;
					}
					pos++;
				}
			}
			
			// Prelucram bucata citita
			for(int i=pos; i<size; i++){
				if (isSeparator(sequence[i])){
					addWord(word);
					word = "";
					continue;
				}
				word += (char)sequence[i];
				
			}
			
			// Citim si in afara fragmentului pana intalnim primul delimitator.
			// Asta daca nu cumva ne aflam deja la sfarsitul fisierului (deci 
			// verificam acest lucru).
			if (offsetEnd!=end-1){
				while(true){
					character = file.readByte();
					if (isSeparator(character)){
						addWord(word);
						break;
					}
					word += (char)character;
					
				}
			}
			
			
			
			file.close();
		} catch (FileNotFoundException e){
			System.out.println("Fisierul " + filename + " nu a putut fi deschis.");
		} catch (IOException e){
			System.out.println("Eroare la citire din fisierulll " + filename + ".");
		}
	}
	
	/** Pentru orice caracter care este in afara intervalului a-z(incluzand aici si 
	 * upper case) metoda considera acel caracter un delimitator. */
	public boolean isSeparator(byte character){
		char letter = (char)character;
		if ((letter>='a' && letter<='z') || (letter>='A' && letter<='Z')){
			return false;
		} else {
			return true;
		}
	}
	
	/** Adauga un cuvant in hashmapul local ce contine perechi de tip (cheie, valoare).
	 *  Daca cumva cuvantul se gaseste deja in hashmap atunci frecventa doar se
	 *  incrementeaza. */
	public void addWord(String word){
		int frequency = 0;
		word = word.toLowerCase();
		if (word!=""){
			if (words.containsKey(word)){
				frequency = words.get(word);
			}
			words.put(word, frequency+1);
		}
	}

	public String getFilename() {
		return filename;
	}

	public HashMap<String, Integer> getWords() {
		return words;
	}
	
	
	
}
