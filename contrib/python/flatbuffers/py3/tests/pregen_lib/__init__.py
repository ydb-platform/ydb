import flatbuffers

from .MyGame.Sample import (
    Color,
    Equipment,
    Monster,
    Vec3,
    Weapon,
)

# Example of how to use FlatBuffers to create and read binary buffers.

def sample():
    builder = flatbuffers.Builder(0)

    # Create some weapons for our Monster ('Sword' and 'Axe').
    weapon_one = builder.CreateString('Sword')
    weapon_two = builder.CreateString('Axe')

    MyGame.Sample.Weapon.WeaponStart(builder)
    MyGame.Sample.Weapon.WeaponAddName(builder, weapon_one)
    MyGame.Sample.Weapon.WeaponAddDamage(builder, 3)
    sword = MyGame.Sample.Weapon.WeaponEnd(builder)

    MyGame.Sample.Weapon.WeaponStart(builder)
    MyGame.Sample.Weapon.WeaponAddName(builder, weapon_two)
    MyGame.Sample.Weapon.WeaponAddDamage(builder, 5)
    axe = MyGame.Sample.Weapon.WeaponEnd(builder)

    # Serialize the FlatBuffer data.
    name = builder.CreateString('Orc')

    MyGame.Sample.Monster.MonsterStartInventoryVector(builder, 10)
    # Note: Since we prepend the bytes, this loop iterates in reverse order.
    for i in reversed(range(0, 10)):
        builder.PrependByte(i)
    inv = builder.EndVector()

    MyGame.Sample.Monster.MonsterStartWeaponsVector(builder, 2)
    # Note: Since we prepend the data, prepend the weapons in reverse order.
    builder.PrependUOffsetTRelative(axe)
    builder.PrependUOffsetTRelative(sword)
    weapons = builder.EndVector()

    pos = MyGame.Sample.Vec3.CreateVec3(builder, 1.0, 2.0, 3.0)

    MyGame.Sample.Monster.MonsterStart(builder)
    MyGame.Sample.Monster.MonsterAddPos(builder, pos)
    MyGame.Sample.Monster.MonsterAddHp(builder, 300)
    MyGame.Sample.Monster.MonsterAddName(builder, name)
    MyGame.Sample.Monster.MonsterAddInventory(builder, inv)
    MyGame.Sample.Monster.MonsterAddColor(builder,
                                          MyGame.Sample.Color.Color().Red)
    MyGame.Sample.Monster.MonsterAddWeapons(builder, weapons)
    MyGame.Sample.Monster.MonsterAddEquippedType(
        builder, MyGame.Sample.Equipment.Equipment().Weapon)
    MyGame.Sample.Monster.MonsterAddEquipped(builder, axe)
    orc = MyGame.Sample.Monster.MonsterEnd(builder)

    builder.Finish(orc)

    # We now have a FlatBuffer that we could store on disk or send over a network.

    # ...Saving to file or sending over a network code goes here...

    # Instead, we are going to access this buffer right away (as if we just
    # received it).

    buf = builder.Output()

    # Note: We use `0` for the offset here, since we got the data using the
    # `builder.Output()` method. This simulates the data you would store/receive
    # in your FlatBuffer. If you wanted to read from the `builder.Bytes` directly,
    # you would need to pass in the offset of `builder.Head()`, as the builder
    # actually constructs the buffer backwards.
    monster = MyGame.Sample.Monster.Monster.GetRootAsMonster(buf, 0)

    # Note: We did not set the `Mana` field explicitly, so we get a default value.
    assert monster.Mana() == 150
    assert monster.Hp() == 300
    assert monster.Name().decode('utf-8') == 'Orc'
    assert monster.Color() == MyGame.Sample.Color.Color().Red
    assert monster.Pos().X() == 1.0
    assert monster.Pos().Y() == 2.0
    assert monster.Pos().Z() == 3.0

    # Get and test the `inventory` FlatBuffer `vector`.
    for i in range(monster.InventoryLength()):
        assert monster.Inventory(i) == i

    # Get and test the `weapons` FlatBuffer `vector` of `table`s.
    expected_weapon_names = ['Sword', 'Axe']
    expected_weapon_damages = [3, 5]
    for i in range(monster.WeaponsLength()):
        assert monster.Weapons(i).Name().decode('utf-8') == expected_weapon_names[i]
        assert monster.Weapons(i).Damage() == expected_weapon_damages[i]

    # Get and test the `equipped` FlatBuffer `union`.
    assert monster.EquippedType() == MyGame.Sample.Equipment.Equipment().Weapon

    # An example of how you can appropriately convert the table depending on the
    # FlatBuffer `union` type. You could add `elif` and `else` clauses to handle
    # the other FlatBuffer `union` types for this field.
    if monster.EquippedType() == MyGame.Sample.Equipment.Equipment().Weapon:
        # `monster.Equipped()` returns a `flatbuffers.Table`, which can be used
        # to initialize a `MyGame.Sample.Weapon.Weapon()`, in this case.
        union_weapon = MyGame.Sample.Weapon.Weapon()
        union_weapon.Init(monster.Equipped().Bytes, monster.Equipped().Pos)

        assert union_weapon.Name().decode('utf-8') == "Axe"
        assert union_weapon.Damage() == 5

    print('The FlatBuffer was successfully created and verified!')
