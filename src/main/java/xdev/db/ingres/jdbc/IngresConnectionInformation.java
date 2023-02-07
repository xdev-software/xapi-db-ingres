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

import com.xdev.jadoth.sqlengine.dbms.DbmsConnectionInformation;

import xdev.db.ConnectionInformation;


public class IngresConnectionInformation extends ConnectionInformation<IngresDbms>
{
	// /////////////////////////////////////////////////////////////////////////
	// constructors //
	// ///////////////////
	
	public IngresConnectionInformation(
		final String host, final int port, final String user,
		final String password, final String database, final String urlExtension,
		final IngresDbms dbmsAdaptor)
	{
		super(host, port, user, password, database, urlExtension, dbmsAdaptor);
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// getters //
	// ///////////////////
	
	/**
	 * Gets the database.
	 *
	 * @return the database
	 */
	public String getDatabase()
	{
		return this.getCatalog();
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// setters //
	// ///////////////////
	
	/**
	 * Sets the database.
	 *
	 * @param database the database to set
	 */
	public void setDatabase(final String database)
	{
		this.setCatalog(database);
	}
	
	// /////////////////////////////////////////////////////////////////////////
	// override methods //
	// ///////////////////
	
	/**
	 * @see DbmsConnectionInformation#createJdbcConnectionUrl()
	 */
	@Override
	public String createJdbcConnectionUrl()
	{
		final String user = this.getUser();
		final String pwd = this.getPassword();
		final String url;
		if((user == null || user.length() == 0) && (pwd == null || pwd.length() == 0))
		{
			url = "jdbc:ingres://" + this.getHost() + ":" + this.getPort() + "/" + this.getCatalog();
		}
		else
		{
			url = "jdbc:ingres://" + this.getHost() + ":" + this.getPort() + "/" + this.getCatalog() + ";UID="
				+ user + ";PWD=" + pwd;
		}
		return this.appendUrlExtension(url);
	}
	
	/**
	 * @return
	 * @see DbmsConnectionInformation#getJdbcDriverClassName()
	 */
	@Override
	public String getJdbcDriverClassName()
	{
		return "com.ingres.jdbc.IngresDriver";
	}
}
