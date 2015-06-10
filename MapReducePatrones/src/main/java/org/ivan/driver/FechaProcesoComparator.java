package org.ivan.driver;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.ivan.writables.ClaveFechaProceso;

public  class FechaProcesoComparator extends WritableComparator {
	
	protected FechaProcesoComparator() {
		super(ClaveFechaProceso.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		
		ClaveFechaProceso cin1 = (ClaveFechaProceso) w1;
		ClaveFechaProceso cin2 = (ClaveFechaProceso) w2;
		
		int cmp = cin1.getFecha().compareTo(cin2.getFecha());
		if (cmp == 0) {
			return cmp;
		}
		
		return -(cin1.getFecha()).compareTo(cin2.getFecha());
	}
}
