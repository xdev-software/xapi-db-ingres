/*
 * SqlEngine Database Adapter Ingres - XAPI SqlEngine Database Adapter for Ingres
 * Copyright © 2003 XDEV Software (https://xdev.software)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package xdev.db.ingres.jdbc;

import static com.xdev.jadoth.sqlengine.SQL.LANG.DEFAULT_VALUES;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation._;
import static com.xdev.jadoth.sqlengine.SQL.Punctuation.dot;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.ASEXPRESSION;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.OMITALIAS;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.UNQUALIFIED;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.indent;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isOmitAlias;
import static com.xdev.jadoth.sqlengine.internal.QueryPart.isSingleLine;
import static com.xdev.jadoth.sqlengine.internal.interfaces.TableExpression.Utils.getAlias;

import com.xdev.jadoth.sqlengine.INSERT;
import com.xdev.jadoth.sqlengine.SELECT;
import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.standard.StandardDMLAssembler;
import com.xdev.jadoth.sqlengine.internal.AssignmentValuesClause;
import com.xdev.jadoth.sqlengine.internal.QueryPart;
import com.xdev.jadoth.sqlengine.internal.SqlColumn;
import com.xdev.jadoth.sqlengine.internal.interfaces.TableExpression;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


public class IngresDMLAssembler extends StandardDMLAssembler<IngresDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	public IngresDMLAssembler(final IngresDbms dbms)
	{
		super(dbms);
	}
	
	@Override
	public StringBuilder assembleColumn(
		final SqlColumn column, final StringBuilder sb,
		final int indentLevel, int flags)
	{
		final TableExpression owner = column.getOwner();
		
		final DbmsAdaptor<?> dbms = this.getDbmsAdaptor();
		final String columnName = column.getColumnName();
		final boolean delimColumn = (dbms.getConfiguration().isDelimitColumnIdentifiers() || QueryPart
			.isDelimitColumnIdentifiers(flags))
			&& (columnName != null && !"*".equals(columnName));
		final char delimiter = dbms.getIdentifierDelimiter();
		
		flags |= QueryPart.bitDelimitColumnIdentifiers(this.getDbmsAdaptor().getConfiguration()
			.isDelimitColumnIdentifiers());
		
		if(owner != null && !QueryPart.isUnqualified(flags))
		{
			this.assembleColumnQualifier(column, sb, flags);
		}
		if(delimColumn)
		{
			sb.append(delimiter);
		}
		QueryPart.assembleObject(column.getExpressionObject(), this, sb, indentLevel, flags);
		if(delimColumn)
		{
			sb.append(delimiter);
		}
		return sb;
	}
	
	@Override
	public StringBuilder assembleColumnQualifier(
		final SqlColumn column, final StringBuilder sb,
		final int flags)
	{
		final TableExpression owner = column.getOwner();
		String qualifier = getAlias(owner);
		if(qualifier == null || QueryPart.isQualifyByTable(flags))
		{
			if(owner instanceof SqlTableIdentity)
			{
				return this.assembleTableIdentifier((SqlTableIdentity)owner, sb, 0, flags).append(dot);
			}
			else
			{
				qualifier = owner.toString();
			}
		}
		final char delimiter = this.getDbmsAdaptor().getIdentifierDelimiter();
		return sb.append(delimiter).append(qualifier).append(delimiter).append(dot);
	}
	
	@Override
	public StringBuilder assembleTableIdentifier(
		final SqlTableIdentity table, final StringBuilder sb,
		final int indentLevel, final int flags)
	{
		final DbmsAdaptor<?> dbms = this.getDbmsAdaptor();
		
		final SqlTableIdentity.Sql sql = table.sql();
		final String schema = sql.schema;
		final String name = sql.name;
		final char delimiter = dbms.getIdentifierDelimiter();
		
		if(schema != null)
		{
			sb.append(delimiter)
				.append(schema)
				.append(delimiter)
				.append(dot);
		}
		sb.append(delimiter)
			.append(name)
			.append(delimiter);
		
		if(!isOmitAlias(flags))
		{
			final String alias = sql.alias;
			if(alias != null && alias.length() > 0)
			{
				sb.append(_)
					.append(delimiter)
					.append(alias)
					.append(delimiter);
			}
		}
		return sb;
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * @see StandardDMLAssembler#assembleSELECT(SELECT, StringBuilder, int, int, String, String)
	 */
	@Override
	protected StringBuilder assembleSELECT(
		final SELECT query,
		final StringBuilder sb,
		final int indentLevel,
		final int flags,
		final String clauseSeperator,
		final String newLine)
	{
		indent(sb, indentLevel, isSingleLine(flags)).append(query.keyword());
		this.assembleSelectDISTINCT(query, sb, indentLevel, flags);
		this.assembleSelectItems(query, sb, flags, indentLevel, newLine);
		this.assembleSelectSqlClauses(query, sb, indentLevel, flags | ASEXPRESSION, clauseSeperator,
			newLine);
		this.assembleAppendSELECTs(query, sb, indentLevel, flags, clauseSeperator, newLine);
		this.assembleSelectRowLimit(query, sb, flags, clauseSeperator, newLine, indentLevel);
		return sb;
	}
	
	/**
	 * @see StandardDMLAssembler#assembleSelectRowLimit(SELECT, StringBuilder, int, String, String, int)
	 */
	@Override
	protected StringBuilder assembleSelectRowLimit(
		final SELECT query,
		final StringBuilder sb,
		final int flags,
		final String clauseSeperator,
		final String newLine,
		final int indentLevel)
	{
		final Integer offset = query.getOffsetSkipCount();
		final Integer limit = query.getFetchFirstRowCount();
		
		if(offset != null && limit != null)
		{
			sb.append(newLine)
				.append(clauseSeperator)
				.append("OFFSET ")
				.append(offset)
				.append(" FETCH FIRST ")
				.append(limit)
				.append(" ROWS ONLY");
		}
		else if(limit != null)
		{
			sb.append(newLine)
				.append(clauseSeperator)
				.append("FETCH FIRST ")
				.append(limit)
				.append(" ROWS ONLY");
		}
		else if(offset != null)
		{
			sb.append(newLine)
				.append(clauseSeperator)
				.append("OFFSET ")
				.append(offset);
		}
		
		return sb;
	}
	
	@Override
	protected StringBuilder assembleINSERT(
		final INSERT query, final StringBuilder sb, final int flags,
		final String clauseSeperator, final String newLine, final int indentLevel)
	{
		indent(sb, indentLevel, isSingleLine(flags)).append(query.keyword()).append(_INTO_);
		
		this.assembleTableIdentifier(query.getTable(), sb, indentLevel, flags | OMITALIAS);
		sb.append(newLine);
		
		this.assembleAssignmentColumnsClause(query, query.getColumnsClause(), sb, indentLevel, flags
			| UNQUALIFIED);
		sb.append(newLine);
		
		final SELECT valueSelect = query.filterSelect();
		if(valueSelect != null)
		{
			sb.append(clauseSeperator);
			QueryPart.assembleObject(valueSelect, this, sb, indentLevel, flags);
		}
		else
		{
			final AssignmentValuesClause values = query.getValuesClause();
			if(values != null)
			{
				this.assembleAssignmentValuesClause(query, values, sb, indentLevel, flags);
			}
			else
			{
				indent(sb, indentLevel, isSingleLine(flags)).append(DEFAULT_VALUES);
			}
		}
		
		return sb;
	}
}
