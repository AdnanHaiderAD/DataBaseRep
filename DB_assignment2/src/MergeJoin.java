/*
 * Created on Feb 11, 2004 by sviglas
 *
 * Modified on Feb 17, 2009 by sviglas
 *
 * This is part of the attica project.  Any subsequent modification
 * of the file should retain this disclaimer.
 * 
 * University of Edinburgh, School of Informatics
 */
package org.dejave.attica.engine.operators;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import java.io.IOException;

import org.dejave.attica.model.Relation;

import org.dejave.attica.engine.predicates.Predicate;
import org.dejave.attica.engine.predicates.PredicateEvaluator;
import org.dejave.attica.engine.predicates.PredicateTupleInserter;

import org.dejave.attica.storage.IntermediateTupleIdentifier;
import org.dejave.attica.storage.RelationIOManager;
import org.dejave.attica.storage.StorageManager;
import org.dejave.attica.storage.StorageManagerException;
import org.dejave.attica.storage.Tuple;

import org.dejave.attica.storage.FileUtil;

/**
 * MergeJoin: Implements a merge join. The assumptions are that the
 * input is already sorted on the join attributes and the join being
 * evaluated is an equi-join.
 *
 * @author sviglas
 * 
 */
public class MergeJoin extends NestedLoopsJoin {
	
    /** The name of the temporary file for the output. */
    private String outputFile;
    
    /** The relation manager used for I/O. */
    private RelationIOManager outputMan;
    
    /** The pointer to the left sort attribute. */
    private int leftSlot;
	
    /** The pointer to the right sort attribute. */
    private int rightSlot;

    /** The iterator over the output file. */
    private Iterator<Tuple> outputTuples;

    /** Reusable output list. */
    private List<Tuple> returnList;
	
    /**
     * Constructs a new mergejoin operator.
     * 
     * @param left the left input operator.
     * @param right the right input operator.
     * @param sm the storage manager.
     * @param leftSlot pointer to the left sort attribute.
     * @param rightSlot pointer to the right sort attribute.
     * @param predicate the predicate evaluated by this join operator.
     * @throws EngineException thrown whenever the operator cannot be
     * properly constructed.
     */
    public MergeJoin(Operator left, 
                     Operator right,
                     StorageManager sm,
                     int leftSlot,
                     int rightSlot,
                     Predicate predicate) 
	throws EngineException {
        
        super(left, right, sm, predicate);
       
        this.leftSlot = leftSlot;
        this.rightSlot = rightSlot;
        returnList = new ArrayList<Tuple>(); 
        try {
            initTempFiles();
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not instantiate " +
                                                     "merge join");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // MergeJoin()


    /**
     * Initialise the temporary files -- if necessary.
     * 
     * @throws StorageManagerException thrown whenever the temporary
     * files cannot be initialised.
     */
    protected void initTempFiles() throws StorageManagerException {
        ////////////////////////////////////////////
        //
        // initialise the temporary files here
        // make sure you throw the right exception
        //
        ////////////////////////////////////////////
        outputFile = FileUtil.createTempFileName();
    } // initTempFiles()

    
    /**
     * Sets up this merge join operator.
     * 
     * @throws EngineException thrown whenever there is something
     * wrong with setting this operator up.
     */
    
    @SuppressWarnings("unchecked")
	@Override
    protected void setup() throws EngineException {
        try {
            System.out.println("done");
            ////////////////////////////////////////////
            //
            // YOUR CODE GOES HERE
            //extract the sorted left relation
            ExternalSort leftOp = (ExternalSort)getInputOperator(0);
            leftOp.setup();
            RelationIOManager leftinp = leftOp.getOutputmanager();
            
            /* extract the sorted right relation */
            ExternalSort rightOp = (ExternalSort)getInputOperator(1);
            rightOp.setup();
            RelationIOManager rightinp = rightOp.getOutputmanager();
            
            outputFile = FileUtil.createTempFileName();
            getStorageManager().createFile(outputFile);
            outputMan = new RelationIOManager(getStorageManager(), 
                                              getOutputRelation(), 
                                              outputFile);
            /*Note: For merge-join to work, the tuples in the input related must be sorted with respect to the value of the join attribute. Thus the indexes provided by the leftSlot and
             * rightSlot can be used to advance the relational pointers.
             */
            
            /* execute mergejoin*/
            Iterator<Tuple> leftIterator =leftinp.tuples().iterator(); // iterator on the tuples of the left input operator
            Iterator<Tuple> rightIterator =rightinp.tuples().iterator(); // iterator on the tuples of the right input operator
            Tuple leftTup = leftIterator.next();
        	Tuple rightTup = rightIterator.next();
            while (leftIterator.hasNext() && rightIterator.hasNext()){
            	
            	int comparison = leftTup.getValue(leftSlot).compareTo(rightTup.getValue(rightSlot));
            	/* checks whether left.a <right.a*/
            	while (comparison < 0){
            		if (leftIterator.hasNext()){
            		leftTup = leftIterator.next();}//advance the pointer of the left input relation
            		}
            	/* checks whether left.a >right.a*/
            	while (comparison>0){
            		if (rightIterator.hasNext()){
            			rightTup =rightIterator.next();}/*advance the pointer of the right input relation*/
            	}
            	/* creates a temporary file to store a group in  the 'right' relation*/
            	String tempfileName= FileUtil.createTempFileName();
        		getStorageManager().createFile(tempfileName);
        		RelationIOManager tmpMan =new RelationIOManager(getStorageManager(),rightOp.getOutputRelation(),tempfileName); 
        		Tuple nextTup=null;
            	while (comparison==0){
            	    nextTup=rightIterator.next();
            		tmpMan.insertTuple(rightTup);
            		boolean moreTup=true;
            		while(moreTup){
            			
            			if (rightTup.getValue(rightSlot)==nextTup.getValue(rightSlot)){
            				//stores elements corresponding to a group (in the right relation)  in a temporary file*/
            				tmpMan.insertTuple(nextTup);}
            			else {break;}
            			if (rightIterator.hasNext()){
            				nextTup=  rightIterator.next();}else{moreTup=false;}
            		}	
            		
            		Iterator<Tuple>storedTuples = tmpMan.tuples().iterator();
            		/*while in group*/
            		while (storedTuples.hasNext()){
            			outputMan.insertTuple(combineTuples(leftTup,storedTuples.next()));//produce results
            		}
            		/*if 'left' contains more tuples,advance the pointer of the left relation*/
            		if (leftIterator.hasNext()){
            		leftTup =leftIterator.next();
            		 comparison=leftTup.getValue(leftSlot).compareTo(rightTup.getValue(rightSlot));  }else{break;}
            		 
            		}
            	if (rightTup!=null){
            		//candidate to begin next group
               	   rightTup=nextTup;}		
            	//clean up
            	 getStorageManager().deleteFile(tempfileName);
            	}
            
            /*Drawbacks: At the end of the iteration of the topmost while loop, if there arises a case that the pointers both relations point to the last elements,the topmost while loop exists. 				*In situations where the lowermost pointers point to tuples that satisfy the join predicate, the merge-join algorithm will fail to produce this result.. Thus, this added 				*if clause is added to overcome the deficiency */
             if (leftTup.getValue(leftSlot).compareTo(rightTup.getValue(rightSlot))==0){
            	 outputMan.insertTuple(combineTuples(leftTup,rightTup)); 
             }
             
            	//clean up the temporary files storing the inputs.
               getStorageManager().deleteFile(leftinp.getFileName());
               getStorageManager().deleteFile(rightinp.getFileName());
            		
            	
           
            
            
            ////////////////////////////////////////////
            
            ////////////////////////////////////////////
            //
            // the output should reside in the output file
            //
            ////////////////////////////////////////////

            //
            // you may need to uncomment the following lines if you
            // have not already instantiated the manager -- it all
            // depends on how you have implemented the operator
            //
            //outputMan = new RelationIOManager(getStorageManager(), 
            //                                  getOutputRelation(),
            //                                  outputFile);

            // open the iterator over the output
            outputTuples = outputMan.tuples().iterator();
        }
        catch (IOException ioe) {
            throw new EngineException("Could not create page/tuple iterators.",
                                      ioe);
        }
        catch (StorageManagerException sme) {
            EngineException ee = new EngineException("Could not store " + 
                                                     "intermediate relations " +
                                                     "to files.");
            ee.setStackTrace(sme.getStackTrace());
            throw ee;
        }
    } // setup()
    
    
   
  
    

    /**
     * Inner method to propagate a tuple.
     * 
     * @return an array of resulting tuples.
     * @throws EngineException thrown whenever there is an error in
     * execution.
     */
    @Override
    protected List<Tuple> innerGetNext () throws EngineException {
        try {
            returnList.clear();
            if (outputTuples.hasNext()) returnList.add(outputTuples.next());
            else returnList.add(new EndOfStreamTuple());
            return returnList;
        }
        catch (Exception sme) {
            throw new EngineException("Could not read tuples "
                                      + "from intermediate file.", sme);
        }
    } // innerGetNext()


    /**
     * Inner tuple processing.  Returns an empty list but if all goes
     * well it should never be called.  It's only there for safety in
     * case things really go badly wrong and I've messed things up in
     * the rewrite.
     */
    @Override
    protected List<Tuple> innerProcessTuple(Tuple tuple, int inOp)
	throws EngineException {
        
        return new ArrayList<Tuple>();
    }  // innerProcessTuple()
/* the outputFile in the superclass NestedLoopsJoin is declared private and hence is not inherited by this class. 
 * Thus when the cleanup() method in NestedLoopsJoin cannot be used to clean up the temporary outputFile of Mergejoin. 
 * Hence the method has to be overridden.*/
    protected void cleanup() throws EngineException {
        try {
           
            getStorageManager().deleteFile(outputFile);
        }
        catch (StorageManagerException sme) {
        	System.out.println(outputFile);
            throw new EngineException("Could not clean up final output", sme);
        }
    } // cleanup()
    /**
     * Textual representation
     */
    protected String toStringSingle () {
        return "mj <" + getPredicate() + ">";
    } // toStringSingle()

} // MergeJoin
