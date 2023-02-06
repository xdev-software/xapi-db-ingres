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




import com.xdev.jadoth.sqlengine.dbms.DbmsAdaptor;
import com.xdev.jadoth.sqlengine.dbms.SQLExceptionParser;
import com.xdev.jadoth.sqlengine.interfaces.ConnectionProvider;
import com.xdev.jadoth.sqlengine.internal.DatabaseGateway;
import com.xdev.jadoth.sqlengine.internal.tables.SqlTableIdentity;


public class IngresDbms
		extends
		DbmsAdaptor.Implementation<IngresDbms, IngresDMLAssembler, IngresDDLMapper, IngresRetrospectionAccessor, IngresSyntax>
{
	// /////////////////////////////////////////////////////////////////////////
	// constants //
	// ///////////////////

	/** The Constant MAX_VARCHAR_LENGTH. 1 GB */
	protected static final int	MAX_VARCHAR_LENGTH		= 1000000000;

	protected static final char	IDENTIFIER_DELIMITER	= '"';


	// /////////////////////////////////////////////////////////////////////////
	// static methods //
	// /////////////////
	
	public static ConnectionProvider<IngresDbms> singleConnection(final String host,
			final int port, final String user, final String password, final String database, final String properties)
	{
		return new ConnectionProvider.Body<IngresDbms>(new IngresConnectionInformation(
			host,
			port,
			user,
			password,
			database,
			properties,
			new IngresDbms())
		);
	}


	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////

	public IngresDbms()
	{
		this(new IngresExceptionParser());
	}


	public IngresDbms(final SQLExceptionParser sqlExceptionParser)
	{
		super(sqlExceptionParser,false);
		this.setRetrospectionAccessor(new IngresRetrospectionAccessor(this));
		this.setDMLAssembler(new IngresDMLAssembler(this));
		this.setDdlMapper(new IngresDDLMapper(this));
		this.getConfiguration().setDelimitTableIdentifiers(true);
		this.getConfiguration().setDelimitColumnIdentifiers(true);
	}


	/**
	 * @see DbmsAdaptor#createConnectionInformation(String, int, String, String, String, String)
	 */
	@Override
	public IngresConnectionInformation createConnectionInformation(final String host,
			final int port, final String user, final String password, final String catalog, final String properties)
	{
		return new IngresConnectionInformation(host,port,user,password,catalog,properties, this);
	}


	@Override
	public boolean supportsOFFSET_ROWS()
	{
		return true;
	}


	/**
	 * @see DbmsAdaptor#updateSelectivity(SqlTableIdentity)
	 */
	@Override
	public Object updateSelectivity(final SqlTableIdentity table)
	{
		return null;
	}


	/**
	 * @see DbmsAdaptor#assembleTransformBytes(byte[], StringBuilder)
	 */
	@Override
	public StringBuilder assembleTransformBytes(final byte[] bytes, final StringBuilder sb)
	{
		return sb;
	}


	/**
	 * @see DbmsAdaptor#initialize(DatabaseGateway)
	 */
	@Override
	public void initialize(final DatabaseGateway<IngresDbms> dbc)
	{
	}


	/**
	 * @see DbmsAdaptor#rebuildAllIndices(String)
	 */
	@Override
	public Object rebuildAllIndices(final String fullQualifiedTableName)
	{
		return null;
	}


	/**
	 * @see DbmsAdaptor#getMaxVARCHARlength()
	 */
	@Override
	public int getMaxVARCHARlength()
	{
		return MAX_VARCHAR_LENGTH;
	}


	@Override
	public char getIdentifierDelimiter()
	{
		return IDENTIFIER_DELIMITER;
	}
}
