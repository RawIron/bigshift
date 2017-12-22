module BigShift
  class RedshiftUnloader
    def initialize(redshift_connection, aws_credentials, options={})
      @redshift_connection = redshift_connection
      @aws_credentials = aws_credentials
      @logger = options[:logger] || NullLogger::INSTANCE
      @time_column = options[:timecolumn] || 'timestamp'
      @partition_day = options[:partition]
      @cutoff_day = options[:cutoff]
    end

    def unload_to(schema_name, table_name, s3_uri, options={})
      table_schema = RedshiftTableSchema.new(schema_name, table_name, @redshift_connection)
      credentials_string = "aws_access_key_id=#{@aws_credentials.access_key_id};aws_secret_access_key=#{@aws_credentials.secret_access_key}"

      if @partition_day
          partition_date = Date.parse @partition_day
          from_day = partition_date.strftime("%Y-%m-%d 00:00:00.000")
          to_day = partition_date.next_day(1).strftime("%Y-%m-%d 00:00:00.000")
          select_where = %Q< WHERE #{time_column} \>= '#{from_day}' AND #{time_column} \< '#{to_day}'>
          @logger.info(sprintf('Unloading day %s to %s', from_day, to_day))
      elsif @partition_day and @cutoff_day
          cutoff_date = Date.parse @cutoff_day
          cutoff_at = cutoff_date.strftime("%Y-%m-%d 00:00:00.000")
          select_where << %Q< AND DATEADD(second, CAST(split_part(signature, ':', 2) AS bigint), '1970-01-01') \< '#{cutoff_at}'>
          @logger.info(sprintf('Cutoff at %s', cutoff_at))
      else
          select_where = ""
      end

      select_sql = 'SELECT '
      select_sql << table_schema.columns.map(&:to_sql).join(', ')
      select_sql << %Q< FROM "#{schema_name}"."#{table_name}">
      select_sql << select_where
      select_sql.gsub!('\'') { |s| '\\\'' }

      unload_sql = %Q< UNLOAD ('#{select_sql}')>
      unload_sql << %Q< TO '#{s3_uri}'>
      unload_sql << %Q< CREDENTIALS '#{credentials_string}'>
#      unload_sql << %q< MANIFEST>
      unload_sql << %q< DELIMITER '\t'>
      unload_sql << %q< GZIP> if options[:compression] || options[:compression].nil?
      unload_sql << %q< ALLOWOVERWRITE> if options[:allow_overwrite]

      @logger.info(sprintf('Unloading Redshift table %s to %s', table_name, s3_uri))
      @redshift_connection.exec(unload_sql)
      @logger.info(sprintf('Unload of %s complete', table_name))
    end
  end
end
