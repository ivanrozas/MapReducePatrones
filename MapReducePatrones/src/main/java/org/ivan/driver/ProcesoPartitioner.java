package org.ivan.driver;

import org.apache.hadoop.mapreduce.Partitioner;
import org.ivan.writables.ClaveFechaProceso;
import org.ivan.writables.ValorProcesoCantidad;

public class ProcesoPartitioner extends Partitioner<ClaveFechaProceso, ValorProcesoCantidad> {
	
	private static final String PARTITIONER_KERNEL = "kernel";

	@Override
	public int getPartition(ClaveFechaProceso key, ValorProcesoCantidad value, int numPartitions) {
		
	 	String proceso = key.getProceso().toString();
	 	
		if(proceso.equals(PARTITIONER_KERNEL)){
			return 0;
		}else{
			return 1;
		}
	}
}
