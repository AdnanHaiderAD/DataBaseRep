/*
 * Created on Jan 18, 2004 by sviglas
 *
 * Modified on Dec 24, 2008 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;

import org.dejave.attica.model.Relation;
import org.dejave.attica.storage.Tuple;

import org.dejave.attica.storage.Page;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;

import org.dejave.attica.storage.FileUtil;

import practice.Test_tuples;

/**
 * ExternalSort: Your implementation of sorting.
 *
 * @author sviglas
 */
public class ExternalSort extends UnaryOperator {
    
    /** The storage manager for this operator. */
    private StorageManager sm;
    
    /** The name of the temporary file for the output. */
    private String outputFile;
	
    /** The manager that undertakes output relation I/O. */
    private RelationIOManager outputMan;
	
    /** The slots that act as the sort keys. */
    private int [] slots;
	
    /** Number of buffers (i.e., buffer pool pages and 
     * output files). */
    private int buffers;
    
    /** number of pages in the input operator **/
    private int numOfPages;

    /** Iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable tuple list for returns. */
    private LinkedList<Tuple> returnList;
    
   
     
     private Comparator<Tuple> TupleComparator  =null;
    
    /**
     * Constructs a new external sort operator.
     * 
     * @param operator the input operator.
     * @param sm the storage manager.
     * @param slots the indexes of the sort keys.
     * @param buffers the number of buffers (i.e., run files) to be
     * used for the sort.
     * @throws EngineException thrown whenever the sort operator
     * cannot be properly initialized.
     */
    public ExternalSort(Operator operator, StorageManager sm,
                        int [] slots, int buffers) 
	throws EngineException {
        
        super(operator);
        this.sm = sm;
        this.slots = slots;
        this.buffers = buffers;
        /*to sort tuples before generating the first runs*/
        TupleComparator= new Comparator<Tuple>() {
       	 
    	    public int compare(Tuple tup1, Tuple tup2) {
    	    	return compareTuples(tup1,tup2);
    	    }
         } ;
         returnList= new LinkedList<Tuple>();
        
          
       
    } // ExternalSort()
	

    /**
     * Initialises the temporary files, according to the number
     * of buffers.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     * @throws EngineException 
     */
    protected String initTempFiles() throws  EngineException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        // in the event of an error
        //
        // for the time being, the only file we
        // know of is the output file
        //
        ////////////////////////////////////////////
    	String tempFile=null;
    	try{
         tempFile = FileUtil.createTempFileName();
        sm.createFile(tempFile);
    	}
        catch (StorageManagerException sme) {
            throw new EngineException("Could not instantiate "
                                      + "External Sort", sme);
        }
        return tempFile;
    } // initTempFiles()

    
    /**
     * Sets up this external sort operator.
     * 
     * @throws EngineException thrown whenever there is something wrong with
     * setting this operator up
     */
    public void setup() throws EngineException {
       
        try {
            ////////////////////////////////////////////
            //
            // this is a blocking operator -- store the input
            // in a temporary file and sort the file
            //
            ////////////////////////////////////////////
            
            ////////////////////////////////////////////
            //
            // store the input
        	 String inputFile=FileUtil.createTempFileName();
        	 getStorageManager().createFile(inputFile);
        	 Relation Rel = getInputOperator().getOutputRelation();
             RelationIOManager Man = new RelationIOManager(getStorageManager(), Rel, inputFile);
             boolean done = false;
             while (! done) {
                 Tuple tuple = getInputOperator().getNext();
                 if (tuple != null) {
                     done = (tuple instanceof EndOfStreamTuple);
                     if (! done) Man.insertTuple(tuple);
                 }
             }
             /**Initial list of generated runs**/
             LinkedList<RelationIOManager> runs_1= generateIniRuns( Man, Rel);
            
             //perform External Sort
             /* number of remaining passes*/
             int numOfPasses = (int)Math.ceil(Math.log(numOfPages)/Math.log(buffers)) ;
             outputMan= perform_externalSort(runs_1,numOfPasses,Rel);
             outputFile=outputMan.getFileName();
             cleanup(runs_1);
             
            ////////////////////////////////////////////
            
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////
            
            
            outputTuples = outputMan.tuples().iterator();
        }
        catch (Exception sme) {
            throw new EngineException("Could not store and sort"
                                      + "intermediate files.", sme);
        }
    } // setup()

    private RelationIOManager perform_externalSort(LinkedList<RelationIOManager> runs_1,int numOfPasses, Relation rel) {
    	LinkedList<RelationIOManager> intermediateList = new LinkedList<RelationIOManager>();
    	for (int i=0;i<numOfPasses;i++){
    		int counter =0;
    		int arraySize=(runs_1.size()>(buffers-1))?buffers-1:runs_1.size();
    		while (counter<runs_1.size()){
    				RelationIOManager[] pass = new RelationIOManager[arraySize];
    				for (int k=0;k<arraySize;k++){
    					pass[k]= runs_1.get(counter+k);
    				}
    				counter= counter+arraySize;
    				ArrayList<Iterator<Page>> pageIterators = new ArrayList<Iterator<Page>>();
    					for (int k=0;k<arraySize;k++){
    						
								try {
									pageIterators.add(k,  pass[k].pages().iterator());
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (StorageManagerException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							
    					}
    					
    					Page[] bufferPages= new Page[arraySize];
    					for (int k=0;k<arraySize;k++){
        						bufferPages[k]=  pageIterators.get(k).next();
        					}
    					
    					ArrayList<Iterator<Tuple>> tupleIterators = new ArrayList<Iterator<Tuple>>();
    					for (int k=0;k<arraySize;k++){
    						tupleIterators.add(k,  bufferPages[k].iterator());
							
    					}
    					
    					Tuple[] tuples = new Tuple[arraySize];
    					for (int k=0;k<arraySize;k++){
    						tuples[k]= tupleIterators.get(buffers).next();
    					}
    					String tempFile=null;
    					try {
							tempFile=initTempFiles();
						} catch (EngineException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						RelationIOManager intermedOutput = new RelationIOManager(getStorageManager(),rel,tempFile);
						int pagesEmpty=0;
						while (pagesEmpty <arraySize){
								Tuple tuple_w= tuples[0];
								int index=0 ;
								for (int k=0;k<arraySize;k++){
										if (tuples[k]==null){
											if (tupleIterators.get(k).hasNext()){
												tuples[k]= tupleIterators.get(k).next();
											}else if (pageIterators.get(k).hasNext()){
												bufferPages[k]= pageIterators.get(k).next();
												tupleIterators.remove(k);
												tupleIterators.add(index,bufferPages[k].iterator());
												tuples[k]= tupleIterators.get(k).next();
									
											}else{
												pagesEmpty++;
												tuples[k]=null;
									
											}
										}
										if (tuples[k]!=null){
											int comparsion = compareTuples(tuples[k],tuple_w);
											if (comparsion <0){
												 tuple_w= tuples[k];
												 index=k;
													}
										}
								   }
										
									try {
										if (tuple_w!=null){
										intermedOutput.insertTuple(tuple_w);}
										tuples[index]=null;
									} catch (StorageManagerException e) {
							// TODO Auto-generated catch block
										e.printStackTrace();
									}
									
						}
						intermediateList.add(intermedOutput);
						
    				}
    			runs_1= intermediateList;
    			intermediateList.clear();
    	}
    	if (runs_1.size()==1){
    		return runs_1.getFirst();
    	}else{
    		System.out.println("THERE IS A BUG In The algorithm");
    		return new RelationIOManager(getStorageManager(),rel,outputFile);
    	}
    		
    		// TODO Auto-generated method stub
		
	}


	/**this method reads the input file in batches of B pages, sorts them in main memory and generates the initial runs**/
    public LinkedList<RelationIOManager> generateIniRuns(RelationIOManager Man,Relation Rel){
    	/**Initial list of generated runs**/
        LinkedList<RelationIOManager> runs_1= new LinkedList<RelationIOManager>();
    	try {
			Iterator<Page> PageIt = Man.pages().iterator();
			returnList.clear();
			int counter=0;
			numOfPages=0;
			while(PageIt.hasNext()){
				Page P= PageIt.next();
				Iterator<Tuple> TupleIt = P.iterator();
				 while (TupleIt.hasNext()){
					  Tuple tuple = TupleIt.next();
					  returnList.add(tuple);
				 }
				 counter++;
				 numOfPages++;
				 if (counter==buffers || !PageIt.hasNext()){
					 Collections.sort(returnList,TupleComparator);
					 returnList.add(new EndOfStreamTuple());
					 String tempFile= initTempFiles(); // create the temporary output file
			            
					 RelationIOManager manRun= new RelationIOManager(getStorageManager(), Rel, tempFile);
					 
		             while (!returnList.isEmpty()) {
		                 Tuple tuple = returnList.removeFirst(); 
		                 manRun.insertTuple(tuple);
		             }
		             runs_1.add(manRun);
		             counter=0;
		             returnList.clear();
					 
				 }
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (StorageManagerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (EngineException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return runs_1;
    	
    }
    
    private StorageManager getStorageManager() {
		// TODO Auto-generated method stub
		return sm;
	}


	/**
     * Cleanup after the sort.
     * 
     * @throws EngineException whenever the operator cannot clean up
     * after itself.
     */
    
    public void cleanup (List<RelationIOManager> runs_1) throws EngineException {
        try {
            ////////////////////////////////////////////
            //
            // deletes the intermediate files:
            // 
            //
            ////////////////////////////////////////////
            
            for (RelationIOManager Man : runs_1){
            sm.deleteFile(Man.getFileName());
            }
        }
        catch (StorageManagerException sme) {
            throw new EngineException("Could not clean up final output.", sme);
        }
    } // cleanup()

    
    public int compareTuples(Tuple tup1, Tuple tup2){
    	for (int i=0;i<slots.length;i++){
    		int comparison =  tup1.getValues().get(slots[i]).compareTo(tup2.getValues().get(slots[i]));
    		if (comparison!=0){
    			return comparison;
    			
    		}
     
    	}
    	return 0;
    }
    
    /**
     * The inner method to retrieve tuples.
     * 
     * @return the newly retrieved tuples.
     * @throws EngineException thrown whenever the next iteration is not 
     * possible.
     */    
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples " +
                                      "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Operator class abstract interface -- never called.
     */
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        return new ArrayList<Tuple>();
    } // innerProcessTuple()

    
    /**
     * Operator class abstract interface -- sets the ouput relation of
     * this sort operator.
     * 
     * @return this operator's output relation.
     * @throws EngineException whenever the output relation of this
     * operator cannot be set.
     */
    protected Relation setOutputRelation() throws EngineException {
        return new Relation(getInputOperator().getOutputRelation());
    } // setOutputRelation()

} // ExternalSort
