# hijri-converter

## ⚠️ DEPRECATED - Use HijriDate instead

This package is **deprecated** and will not receive future updates.

### Migration Required

Install the [HijriDate](https://pypi.org/project/hijridate/) package:

```bash
pip install hijridate==2.3.0
```

Update your imports:

```python
# Old (deprecated)
from hijri_converter import Hijri, Gregorian

# New (recommended)
from hijridate import Hijri, Gregorian
```

The API is identical - only the package name changes.

### Why the change?

The [HijriDate](https://pypi.org/project/hijridate/) package is the actively maintained successor with the same functionality and accuracy.

---

## License

MIT License
