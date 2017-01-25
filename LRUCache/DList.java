
public class DList {
	int capacity;
	int currentSize;
	Node head;
	Node tail;
	
		
	public DList(int capacity){
		this.capacity = capacity;
		this.currentSize = 0;
	}
	
	public Node addPageToList(int data){
		
		Node temp = new Node(data);
		//if cache is empty
		if(head==null){
			head = temp;
			tail = temp;
		}
		//if size is less than capacity
		else if(currentSize<capacity){
			insertAtFront(temp);
		}//if cache is full
		else{
			removeFromTail();
			insertAtFront(temp);
		}
		currentSize++;
		return head;
	}

	public void insertAtFront(Node temp){
		if(head==temp)
			return;
		
		temp.next = head;
		head.pre = temp;
		head = temp;
	}

	public void removeFromTail(){
		tail = tail.pre;
		tail.next = null;
		currentSize--;
	}
	
	public void remove(Node temp){
		if(temp==head){
			return;
		}
		else if(temp==tail){
			removeFromTail();
			return;
		}
		
		temp.pre.next = temp.next;
		temp.next.pre = temp.pre;
		
		currentSize--;
	}
	
	public void print(){
		Node temp = head;
		while(temp!=null){
			System.out.println(temp.data);
			temp = temp.next;
		}
	}


}

class Node{
	
	int data;
	Node pre;
	Node next;

	public Node(int val){
	this.data = val;
	pre = null;
	next = null;
	}
}
	
	
	
	

