import java.util.HashMap;
import java.util.Map;

public class LRUCacheClass {

	private int capacity;
	Map<Integer,Node> hm;
	DList list;
	
	//constructor
	public LRUCacheClass(int capacity){
		
		this.capacity=capacity;
		hm = new HashMap<Integer,Node>();
		list = new DList(capacity);
	}	
	
	//access page
	public void accessPage(int pageNo){
		
		//if page exists
		if(hm.containsKey(pageNo)){
			Node temp = hm.get(pageNo);
			list.remove(temp);
			list.insertAtFront(temp);
		}
		//if page doesn't exist
		else{
			  Node t1 = list.addPageToList(pageNo);
			  hm.put(pageNo,t1);
			}
		}
	
	public void printCacheState(){
		
		list.print();
	}

	public static void main(String[] args) {
		LRUCacheClass lru = new LRUCacheClass(5);
		lru.accessPage(1);
		lru.accessPage(2);
	
		System.out.println("Printing ....");
		lru.printCacheState();
		lru.accessPage(3);
		lru.accessPage(3);
		lru.accessPage(5);
		lru.accessPage(6);
		System.out.println("Printing ....");
		lru.printCacheState();

	}

}
