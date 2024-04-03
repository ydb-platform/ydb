# Custom attributes in tables

You can use [custom attributes](../concepts/datamodel/table.md#users-attr) to store any information and process it in your app or using the CLI.

For example, your application uses a database table. It stores versions of the table's scheme in custom attributes. You need to migrate data when the table scheme changes. To make sure that your application runs a relevant migration, rather than a previous one, when started, access the custom attributes to check the scheme version:

* The version in the attribute is `1`, and your application knows how to work with version `2`. So, you apply the `1`>`2` migration and update the attribute value with `2`.
* In the attribute, the version is `2`, and your application knows how to work with version `2`, so no migration is needed.
* The version in the attribute is `2`, and your application knows how to work with version `1`: you terminate your application with an exception, notifying the user of an attempt to run an older app version against the new data scheme.

When you use custom attributes, you no longer need to store scheme versions in a separate table.

## Set a custom attribute when creating a table {#create}

{% list tabs %}

- Go

   To set a custom attribute when creating the `series` table, pass the `scheme_version` key and the attribute value `1` in the `options.WithAttribute` option of the `CreateTable` method:

   ```go
   err := client.Do(ctx, func(ctx context.Context, s table.Session) error {
     return s.CreateTable(ctx, "episodes",
       options.WithColumn("series_id", types.Optional(types.TypeUint64)),
       options.WithColumn("season_id", types.Optional(types.TypeUint64)),
       options.WithColumn("episode_id", types.Optional(types.TypeUint64)),
       options.WithColumn("title", types.Optional(types.TypeText)),
       options.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
       options.WithAttribute("scheme_version", "1"),
     )
   })
   ```

{% endlist %}

## Set a custom attribute for an existing table {#add}

{% list tabs %}

- Go

   To set a custom attribute for the existing `series` table, pass the `scheme_version` key and the attribute value `1` in the `options.WithAddAttribute` option of the `AlterTable` method:

   ```go
   err = db.Table().Do(ctx,
     func(ctx context.Context, s table.Session) (err error) {
       return s.AlterTable(ctx, path.Join(db.Name(), "series"),
         options.WithAddAttribute("scheme_version", "1"),
       )
     },
   )
   ```

- CLI

   To set a custom attribute for the existing `series` table, pass the `scheme_version` key and the attribute value `1` in the `--attribute` option of the [ydb table attribute add](../reference/ydb-cli/table-attribute-add.md) command:

   ```bash
   ydb table attribute add --attribute scheme_version=1 series
   ```

{% endlist %}

## Update the custom attribute {#alter}

{% list tabs %}

- Go

   To update the custom attribute when changing the table scheme, pass the `scheme_version` key and the new attribute value of `2` in the `WithAlterAttribute` option of the `AlterTable` method:

   ```go
   err = db.Table().Do(ctx,
     func(ctx context.Context, s table.Session) (err error) {
       return s.AlterTable(ctx, path.Join(db.Name(), "series"),
         options.WithAddColumn("air_date", types.Optional(types.TypeUint64)),
         options.WithAlterAttribute("scheme_version", "2"),
       )
     },
   )
   ```

- CLI

   To edit the custom attribute for the existing `series` table, pass the `scheme_version` key and the attribute value `2` in the `--attribute` option of the [ydb table attribute add](../reference/ydb-cli/table-attribute-add.md) command:

   ```bash
   ydb table attribute add --attribute scheme_version=2 series
   ```

{% endlist %}

## View your custom attributes {#view}

{% list tabs %}

- Go

   To retrieve the `series` table scheme, including the custom attributes, use the method `table.Session.DescribeTable()`:

   ```go
   err := c.Do(ctx,
     func(ctx context.Context, s table.Session) error {
       description, err := s.DescribeTable(ctx, path.Join(prefix, "series"))
       if err != nil {
         return err
       }
       for k, v := range description.Attributes {
         log.Println(k, "=", v)
       }
       return nil
     },
   )
   ```

- CLI

   To get the data about the `series` table scheme, including the custom attributes, use the command [ydb scheme describe](../reference/ydb-cli/commands/scheme-describe.md):

   ```bash
   ydb scheme describe series
   ```

   Result:

   ```text
   ...
   Attributes:
   ┌────────────────┬───────┐
   | Name           | Value |
   ├────────────────┼───────┤
   | scheme_version | 2     |
   └────────────────┴───────┘
   ...
   ```

{% endlist %}

## Delete the custom attribute {#drop}

{% list tabs %}

- Go

   To drop the custom attribute, pass the `scheme_version` key in the option `WithDropAttribute` of the `AlterTable` method:

   ```go
   err = db.Table().Do(ctx,
     func(ctx context.Context, s table.Session) (err error) {
       return s.AlterTable(ctx, path.Join(db.Name(), "series"),
         options.WithDropAttribute("scheme_version"),
       )
     },
   )
   ```

- CLI

   To drop the custom attribute, use the `scheme_version` key in the `--attributes` option of the [ydb table attribute drop](../reference/ydb-cli/table-attribute-drop.md) command:

   ```bash
   ydb table attribute drop --attributes scheme_version series
   ```

{% endlist %}