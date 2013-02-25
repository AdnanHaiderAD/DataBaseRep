package practice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;



public class Sorting {

	
	static int[] slots= new int[]{ 1};
	static  Comparator<Test_tuples> Tuplecomparator  = new Comparator<Test_tuples>() {
 
	    public int compare(Test_tuples fruit1, Test_tuples fruit2) {
	    	for (int i=0;i<slots.length;i++){
	    		if (fruit1.tuples.get(slots[i]).compareTo(fruit2.tuples.get(slots[i]))!=0){
	    			return fruit1.tuples.get(slots[i]).compareTo(fruit2.tuples.get(slots[i]));
	    			
	    		}
	     
	    	}
	    	return 0;
	    }
	};
	public static void main(String[] args){
		ArrayList<Comparable> list1 = new ArrayList<Comparable>();
		ArrayList<Comparable> list2 = new ArrayList<Comparable>();
		ArrayList<Comparable> list3 = new ArrayList<Comparable>();
		
		list1.add(0,"hello");
		list2.add(0, "gello");
		list3.add(0,"hello");
		
		list1.add(1,23);
		list2.add(1, 26);
		list3.add(1,55);
		
		
		Test_tuples T1= new Test_tuples("First",list1);
		Test_tuples T2= new Test_tuples("Second",list2);
		Test_tuples T3= new Test_tuples("Third",list3);
		
		List<Test_tuples> L = new ArrayList<Test_tuples>();
		L.add(T1);
		L.add(T2);
		L.add(T3);
		
		Collections.sort(L, Sorting.Tuplecomparator);
		ArrayList<Integer> L2= new ArrayList<Integer>();
		L2.add(1);
		L2.add(2);
		L2.add(3);
		L2.remove(1);
		L2.add(1, 4);
	
		for (int i=0; i< 3;i++){
			System.out.println("element "+i+" is " + L.get(i).tuple_identifier + " " );
			System.out.println("the iterators works as follows "+L2.get(i));
			
		}
		int P =5;
		while ((P--)>0){
		System.out.println(P);
		System.out.println((int)Math.ceil(98.7));
		}
		
	 }
}
