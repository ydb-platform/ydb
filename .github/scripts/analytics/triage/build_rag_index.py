#!/usr/bin/env python3
"""
RAG Index Builder для триажа тестов
Поддержка: Python, C++, ya.make, CMake
Сохраняет: FAISS vectorstore + JSON метаданные
"""

import ast
import json
import os
import pickle
import re
import sys
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
from glob import glob

try:
    # Для langchain >= 1.0 импорты изменились
    try:
        from langchain_community.embeddings import HuggingFaceEmbeddings
        from langchain_community.vectorstores import FAISS
    except ImportError:
        # Fallback для старых версий
        from langchain.embeddings import HuggingFaceEmbeddings
        from langchain.vectorstores import FAISS
    
    try:
        from langchain_core.documents import Document
    except ImportError:
        from langchain.schema import Document
    
    from tqdm import tqdm
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Установите зависимости: pip install langchain langchain-community langchain-core faiss-cpu sentence-transformers tqdm")
    sys.exit(1)


@dataclass
class ChunkMetadata:
    """Метаданные для чанка кода"""
    path: str
    language: str  # cpp, python, yamake, cmake
    kind: str  # test, prod, build, config
    target: Optional[str] = None  # для ya.make: //path/to:target
    symbol: Optional[str] = None  # имя функции/класса/метода
    files: Optional[List[str]] = None  # для ya.make: список SRCS


class RepoIndexer:
    """Индексатор репозитория для RAG"""
    
    # Паттерны по умолчанию (используются если конфиг не найден)
    DEFAULT_INCLUDE_PATTERNS = [
        "**/*.py",
        "**/*.cpp",
        "**/*.cc",
        "**/*.c",
        "**/*.hpp",
        "**/*.h",
        "**/ya.make",
        "**/CMakeLists.txt",
        "**/*.ya"
    ]
    
    DEFAULT_EXCLUDE_PATTERNS = [
        "**/build/**",
        "**/__pycache__/**",
        "**/node_modules/**",
        "**/contrib/**",
        "**/*.pb.*",
        "**/third_party/**",
        "**/.git/**",
        "**/venv/**",
        "**/.venv/**"
    ]
    
    def __init__(self, repo_path: str, output_dir: str = "./rag_index", config_path: Optional[str] = None):
        """
        Инициализация индексатора
        
        Args:
            repo_path: Путь к корню репозитория
            output_dir: Директория для сохранения индекса
            config_path: Путь к конфигурационному файлу (по умолчанию: index_config.json рядом со скриптом)
        """
        self.repo_path = Path(repo_path).resolve()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Загружаем конфигурацию
        self._load_config(config_path)
        
        # Инициализация embeddings (используем lightweight модель)
        print("Инициализация модели embeddings...")
        self.embeddings = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
            model_kwargs={'device': 'cpu'}
        )
        
        self.documents: List[Document] = []
        self.metadata_list: List[ChunkMetadata] = []
    
    def _load_config(self, config_path: Optional[str] = None):
        """Загрузка конфигурации из JSON файла"""
        if config_path is None:
            # Ищем конфиг рядом со скриптом
            script_dir = Path(__file__).parent
            config_path = script_dir / "index_config.json"
        else:
            config_path = Path(config_path)
        
        if config_path.exists():
            print(f"Загрузка конфигурации из {config_path}...")
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                
                self.INCLUDE_PATTERNS = config.get("include_patterns", self.DEFAULT_INCLUDE_PATTERNS)
                self.EXCLUDE_PATTERNS = config.get("exclude_patterns", self.DEFAULT_EXCLUDE_PATTERNS)
                self.EXCLUDE_DIRECTORIES = config.get("exclude_directories", [])
                self.EXCLUDE_FILE_EXTENSIONS = config.get("exclude_file_extensions", [])
                
                print(f"  Загружено паттернов включения: {len(self.INCLUDE_PATTERNS)}")
                print(f"  Загружено паттернов исключения: {len(self.EXCLUDE_PATTERNS)}")
                print(f"  Загружено директорий для исключения: {len(self.EXCLUDE_DIRECTORIES)}")
                print(f"  Загружено расширений для исключения: {len(self.EXCLUDE_FILE_EXTENSIONS)}")
            except Exception as e:
                print(f"Ошибка загрузки конфигурации: {e}")
                print("Используются значения по умолчанию")
                self._use_default_config()
        else:
            print(f"Конфигурационный файл не найден: {config_path}")
            print("Используются значения по умолчанию")
            self._use_default_config()
    
    def _use_default_config(self):
        """Использование конфигурации по умолчанию"""
        self.INCLUDE_PATTERNS = self.DEFAULT_INCLUDE_PATTERNS
        self.EXCLUDE_PATTERNS = self.DEFAULT_EXCLUDE_PATTERNS
        self.EXCLUDE_DIRECTORIES = []
        self.EXCLUDE_FILE_EXTENSIONS = []
    
    def _should_exclude(self, file_path: Path) -> bool:
        """Проверка, нужно ли исключить файл"""
        path_str = str(file_path)
        path_lower = path_str.lower()
        
        # Проверка расширений файлов
        if file_path.suffix.lower() in [ext.lower() for ext in self.EXCLUDE_FILE_EXTENSIONS]:
            return True
        
        # Проверка паттернов исключения
        for pattern in self.EXCLUDE_PATTERNS:
            # Простая проверка паттерна
            pattern_clean = pattern.replace("**/", "").replace("**", "").lower()
            if pattern_clean in path_lower:
                return True
        
        # Проверка директорий в пути
        for exclude_dir in self.EXCLUDE_DIRECTORIES:
            # Проверяем, содержит ли путь эту директорию
            parts = path_str.split(os.sep)
            if exclude_dir in parts or exclude_dir.lower() in [p.lower() for p in parts]:
                return True
        
        return False
    
    def scan_files(self) -> List[Path]:
        """
        Сканирование репозитория и поиск всех релевантных файлов
        
        Returns:
            Список путей к файлам для индексации
        """
        files = []
        
        # Используем rglob для поиска файлов (работает с скрытыми директориями)
        # Преобразуем паттерны в расширения для rglob
        extensions = set()
        special_names = set()
        
        for pattern in self.INCLUDE_PATTERNS:
            if pattern.endswith('/*.py'):
                extensions.add('.py')
            elif pattern.endswith('/*.cpp'):
                extensions.add('.cpp')
            elif pattern.endswith('/*.cc'):
                extensions.add('.cc')
            elif pattern.endswith('/*.c'):
                extensions.add('.c')
            elif pattern.endswith('/*.hpp'):
                extensions.add('.hpp')
            elif pattern.endswith('/*.h'):
                extensions.add('.h')
            elif pattern.endswith('/*.proto'):
                extensions.add('.proto')
            elif pattern.endswith('/*.protobuf'):
                extensions.add('.protobuf')
            elif pattern.endswith('/*.yaml'):
                extensions.add('.yaml')
            elif pattern.endswith('/*.yml'):
                extensions.add('.yml')
            elif pattern.endswith('/*.config'):
                extensions.add('.config')
            elif pattern.endswith('/*.conf'):
                extensions.add('.conf')
            elif pattern.endswith('/*.cfg'):
                extensions.add('.cfg')
            elif pattern.endswith('/*.ini'):
                extensions.add('.ini')
            elif pattern.endswith('/*.toml'):
                extensions.add('.toml')
            elif pattern.endswith('/*.json'):
                extensions.add('.json')
            elif pattern.endswith('/*.md'):
                extensions.add('.md')
            elif pattern.endswith('/*.markdown'):
                extensions.add('.markdown')
            elif 'ya.make' in pattern:
                special_names.add('ya.make')
            elif 'CMakeLists.txt' in pattern:
                special_names.add('CMakeLists.txt')
            elif pattern.endswith('/*.ya'):
                extensions.add('.ya')
        
        # Сканируем репозиторий через rglob
        for ext in extensions:
            for file_path in self.repo_path.rglob(f'*{ext}'):
                if file_path.is_file() and not self._should_exclude(file_path):
                    files.append(file_path)
        
        # Ищем специальные имена файлов
        for name in special_names:
            for file_path in self.repo_path.rglob(name):
                if file_path.is_file() and not self._should_exclude(file_path):
                    files.append(file_path)
        
        # Удаляем дубликаты
        files = list(set(files))
        return sorted(files)
    
    def _determine_kind(self, path: Path) -> str:
        """Определение типа файла: test, prod, build, config"""
        path_str = str(path).lower()
        
        if "test" in path_str or "unittest" in path_str:
            return "test"
        elif path.name in ("ya.make", "CMakeLists.txt", "*.ya"):
            return "build"
        elif "config" in path_str or "cmake" in path_str:
            return "config"
        else:
            return "prod"
    
    def _get_language(self, path: Path) -> str:
        """Определение языка файла"""
        suffix = path.suffix.lower()
        name = path.name.lower()
        
        if suffix == ".py":
            return "python"
        elif suffix in (".cpp", ".cc", ".c", ".hpp", ".h"):
            return "cpp"
        elif name == "ya.make" or suffix == ".ya":
            return "yamake"
        elif name == "cmakelists.txt":
            return "cmake"
        elif suffix in (".proto", ".protobuf"):
            return "protobuf"
        elif suffix in (".yaml", ".yml"):
            return "yaml"
        elif suffix in (".config", ".conf", ".cfg", ".ini", ".toml", ".json"):
            return "config"
        elif suffix in (".md", ".markdown"):
            return "markdown"
        else:
            return "unknown"
    
    def parse_python_file(self, path: Path) -> List[Document]:
        """
        Парсинг Python файла на функции и классы
        
        Args:
            path: Путь к Python файлу
            
        Returns:
            Список Document объектов (чанки)
        """
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                lines = content.split('\n')
        except Exception as e:
            print(f"Ошибка чтения {path}: {e}")
            return []
        
        chunks = []
        has_functions_or_classes = False
        
        try:
            tree = ast.parse(content, filename=str(path))
            
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    has_functions_or_classes = True
                    # Получаем строки начала и конца
                    start_line = node.lineno - 1  # ast использует 1-based, мы 0-based
                    
                    # Для end_line ищем последний дочерний узел
                    end_line = start_line
                    for child in ast.walk(node):
                        if hasattr(child, 'lineno') and child.lineno:
                            end_line = max(end_line, child.lineno - 1)
                    
                    # Если не нашли end_line, берем следующую строку после start
                    if end_line == start_line:
                        end_line = start_line + 10  # fallback
                    
                    # Извлекаем код функции/класса
                    chunk_lines = lines[start_line:end_line + 1]
                    chunk_content = '\n'.join(chunk_lines)
                    
                    # Получаем имя
                    symbol_name = node.name
                    if isinstance(node, ast.FunctionDef):
                        symbol_name = f"def {symbol_name}()"
                    elif isinstance(node, ast.ClassDef):
                        symbol_name = f"class {symbol_name}"
                    
                    # Добавляем докстринг если есть
                    docstring = ast.get_docstring(node)
                    if docstring:
                        chunk_content = f'"""{docstring}"""\n{chunk_content}'
                    
                    # Создаем метаданные
                    metadata_obj = ChunkMetadata(
                        path=str(path.relative_to(self.repo_path)),
                        language="python",
                        kind=self._determine_kind(path),
                        symbol=symbol_name
                    )
                    
                    # Создаем Document
                    doc = Document(
                        page_content=chunk_content,
                        metadata=asdict(metadata_obj)
                    )
                    chunks.append(doc)
                    self.metadata_list.append(metadata_obj)
            
            # Если файл не содержит функций/классов, создаем чанк со всем содержимым
            if not has_functions_or_classes and content.strip():
                # Извлекаем докстринг модуля если есть
                module_docstring = ast.get_docstring(tree)
                chunk_content = content
                if module_docstring:
                    chunk_content = f'"""{module_docstring}"""\n{chunk_content}'
                
                metadata_obj = ChunkMetadata(
                    path=str(path.relative_to(self.repo_path)),
                    language="python",
                    kind=self._determine_kind(path),
                    symbol=f"module {path.name}"
                )
                
                doc = Document(
                    page_content=chunk_content,
                    metadata=asdict(metadata_obj)
                )
                chunks.append(doc)
                self.metadata_list.append(metadata_obj)
        
        except SyntaxError as e:
            print(f"Синтаксическая ошибка в {path}: {e}")
            # Даже при синтаксической ошибке создаем чанк со всем содержимым
            if content.strip():
                metadata_obj = ChunkMetadata(
                    path=str(path.relative_to(self.repo_path)),
                    language="python",
                    kind=self._determine_kind(path),
                    symbol=f"module {path.name}"
                )
                
                doc = Document(
                    page_content=content,
                    metadata=asdict(metadata_obj)
                )
                chunks.append(doc)
                self.metadata_list.append(metadata_obj)
        except Exception as e:
            print(f"Ошибка парсинга {path}: {e}")
            # При ошибке парсинга тоже создаем чанк
            if content.strip():
                metadata_obj = ChunkMetadata(
                    path=str(path.relative_to(self.repo_path)),
                    language="python",
                    kind=self._determine_kind(path),
                    symbol=f"module {path.name}"
                )
                
                doc = Document(
                    page_content=content,
                    metadata=asdict(metadata_obj)
                )
                chunks.append(doc)
                self.metadata_list.append(metadata_obj)
        
        return chunks
    
    def parse_cpp_file(self, path: Path) -> List[Document]:
        """
        Парсинг C++ файла на функции и методы
        
        Args:
            path: Путь к C++ файлу
            
        Returns:
            Список Document объектов (чанки)
        """
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            print(f"Ошибка чтения {path}: {e}")
            return []
        
        chunks = []
        
        # Regex для поиска функций: возвращаемый_тип имя(параметры) { или } {
        # Упрощенный паттерн для функций с телом в {}
        pattern = r'(?m)^(?:\s*(?:template\s*<[^>]+>\s*)?(?:\w+\s*::\s*)?(?:\w+\s+)+)?(\w+)\s*\([^)]*\)\s*(?:const\s*)?\s*\{'
        
        matches = list(re.finditer(pattern, content))
        
        for match in matches:
            func_name = match.group(1)
            start_pos = match.start()
            
            # Находим парную закрывающую скобку
            brace_count = 0
            end_pos = start_pos
            in_string = False
            string_char = None
            
            for i in range(start_pos, len(content)):
                char = content[i]
                
                # Обработка строковых литералов
                if char in ('"', "'") and (i == 0 or content[i-1] != '\\'):
                    if not in_string:
                        in_string = True
                        string_char = char
                    elif char == string_char:
                        in_string = False
                        string_char = None
                    continue
                
                if in_string:
                    continue
                
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_pos = i + 1
                        break
            
            # Извлекаем функцию
            func_content = content[start_pos:end_pos]
            
            # Ищем комментарии перед функцией
            comment_start = max(0, start_pos - 500)
            before_func = content[comment_start:start_pos]
            comments = re.findall(r'/\*.*?\*/|//.*?$', before_func, re.MULTILINE | re.DOTALL)
            if comments:
                func_content = '\n'.join(comments[-3:]) + '\n' + func_content
            
            # Создаем метаданные
            metadata_obj = ChunkMetadata(
                path=str(path.relative_to(self.repo_path)),
                language="cpp",
                kind=self._determine_kind(path),
                symbol=f"{func_name}()"
            )
            
            # Создаем Document
            doc = Document(
                page_content=func_content,
                metadata=asdict(metadata_obj)
            )
            chunks.append(doc)
            self.metadata_list.append(metadata_obj)
        
        return chunks
    
    def parse_yamake_file(self, path: Path) -> List[Document]:
        """
        Парсинг ya.make файла на таргеты
        
        Args:
            path: Путь к ya.make файлу
            
        Returns:
            Список Document объектов (таргеты)
        """
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            print(f"Ошибка чтения {path}: {e}")
            return []
        
        chunks = []
        
        # Паттерны для различных типов таргетов
        target_patterns = [
            r'PROGRAM\s*\([^)]+\)',
            r'LIBRARY\s*\([^)]+\)',
            r'TEST\s*\([^)]+\)',
            r'PY3_LIBRARY\s*\([^)]+\)',
            r'PY3_PROGRAM\s*\([^)]+\)',
            r'PY3_TEST\s*\([^)]+\)',
            r'UNITTEST\s*\([^)]+\)',
            r'PEERDIR\s*\([^)]+\)',
        ]
        
        # Находим все таргеты
        for pattern in target_patterns:
            matches = re.finditer(pattern, content, re.MULTILINE)
            for match in matches:
                target_start = match.start()
                
                # Находим весь блок таргета (до следующего таргета или конца файла)
                # Ищем закрывающую скобку и следующие строки до следующего таргета
                target_end = match.end()
                
                # Ищем конец блока (следующий таргет или пустая строка после закрывающей скобки)
                next_target = re.search(r'(?:^|\n)\s*(?:PROGRAM|LIBRARY|TEST|PY3_|UNITTEST|PEERDIR)', 
                                       content[target_end:], re.MULTILINE)
                if next_target:
                    target_end = target_end + next_target.start()
                else:
                    # Берем следующие 50 строк как fallback
                    lines_after = content[target_end:].split('\n')
                    target_end = target_end + len('\n'.join(lines_after[:50]))
                
                target_content = content[target_start:target_end]
                
                # Извлекаем имя таргета
                target_name_match = re.search(r'\(([^)]+)\)', target_content)
                target_name = target_name_match.group(1).strip() if target_name_match else "unknown"
                
                # Извлекаем SRCS если есть
                srcs_match = re.search(r'SRCS\s*\(([^)]+)\)', target_content, re.MULTILINE)
                private_srcs_match = re.search(r'PRIVATE_SRCS\s*\(([^)]+)\)', target_content, re.MULTILINE)
                
                files_list = []
                if srcs_match:
                    files_list.extend([f.strip() for f in srcs_match.group(1).split()])
                if private_srcs_match:
                    files_list.extend([f.strip() for f in private_srcs_match.group(1).split()])
                
                # Формируем полный путь таргета
                rel_path = path.relative_to(self.repo_path)
                target_full = f"//{str(rel_path.parent).replace(os.sep, '/')}:{target_name}"
                
                # Создаем метаданные
                metadata_obj = ChunkMetadata(
                    path=str(rel_path),
                    language="yamake",
                    kind="build",
                    target=target_full,
                    files=files_list if files_list else None
                )
                
                # Создаем Document
                doc = Document(
                    page_content=target_content,
                    metadata=asdict(metadata_obj)
                )
                chunks.append(doc)
                self.metadata_list.append(metadata_obj)
        
        return chunks
    
    def parse_protobuf_file(self, path: Path) -> List[Document]:
        """
        Парсинг Protobuf файла на сообщения и сервисы
        
        Args:
            path: Путь к .proto файлу
            
        Returns:
            Список Document объектов (чанки)
        """
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            print(f"Ошибка чтения {path}: {e}")
            return []
        
        chunks = []
        
        # Ищем message, enum, service определения
        patterns = [
            (r'message\s+(\w+)\s*\{', 'message'),
            (r'enum\s+(\w+)\s*\{', 'enum'),
            (r'service\s+(\w+)\s*\{', 'service'),
        ]
        
        for pattern, kind in patterns:
            matches = re.finditer(pattern, content, re.MULTILINE)
            for match in matches:
                name = match.group(1)
                start_pos = match.start()
                
                # Находим парную закрывающую скобку
                brace_count = 0
                end_pos = start_pos
                in_string = False
                string_char = None
                
                for i in range(start_pos, len(content)):
                    char = content[i]
                    
                    if char in ('"', "'") and (i == 0 or content[i-1] != '\\'):
                        if not in_string:
                            in_string = True
                            string_char = char
                        elif char == string_char:
                            in_string = False
                            string_char = None
                        continue
                    
                    if in_string:
                        continue
                    
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end_pos = i + 1
                            break
                
                chunk_content = content[start_pos:end_pos]
                
                metadata_obj = ChunkMetadata(
                    path=str(path.relative_to(self.repo_path)),
                    language="protobuf",
                    kind=self._determine_kind(path),
                    symbol=f"{kind} {name}"
                )
                
                doc = Document(
                    page_content=chunk_content,
                    metadata=asdict(metadata_obj)
                )
                chunks.append(doc)
                self.metadata_list.append(metadata_obj)
        
        return chunks
    
    def parse_config_file(self, path: Path) -> List[Document]:
        """
        Парсинг конфигурационного файла
        
        Args:
            path: Путь к конфигурационному файлу
            
        Returns:
            Список Document объектов (чанки)
        """
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            print(f"Ошибка чтения {path}: {e}")
            return []
        
        # Для конфигов просто создаем один чанк со всем содержимым
        metadata_obj = ChunkMetadata(
            path=str(path.relative_to(self.repo_path)),
            language="config",
            kind="config",
            symbol=path.name
        )
        
        doc = Document(
            page_content=content,
            metadata=asdict(metadata_obj)
        )
        
        self.metadata_list.append(metadata_obj)
        return [doc]
    
    def parse_markdown_file(self, path: Path) -> List[Document]:
        """
        Парсинг Markdown файла на секции
        
        Args:
            path: Путь к .md файлу
            
        Returns:
            Список Document объектов (чанки)
        """
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                lines = content.split('\n')
        except Exception as e:
            print(f"Ошибка чтения {path}: {e}")
            return []
        
        chunks = []
        
        # Ищем заголовки (начинающиеся с #)
        current_section = []
        current_title = "Introduction"
        section_start = 0
        
        for i, line in enumerate(lines):
            # Проверяем, является ли строка заголовком
            if line.strip().startswith('#'):
                # Сохраняем предыдущую секцию
                if current_section:
                    section_content = '\n'.join(current_section)
                    if section_content.strip():
                        metadata_obj = ChunkMetadata(
                            path=str(path.relative_to(self.repo_path)),
                            language="markdown",
                            kind="documentation",
                            symbol=current_title
                        )
                        
                        doc = Document(
                            page_content=section_content,
                            metadata=asdict(metadata_obj)
                        )
                        chunks.append(doc)
                        self.metadata_list.append(metadata_obj)
                
                # Начинаем новую секцию
                current_title = line.strip().lstrip('#').strip()
                current_section = [line]
                section_start = i
            else:
                current_section.append(line)
        
        # Сохраняем последнюю секцию
        if current_section:
            section_content = '\n'.join(current_section)
            if section_content.strip():
                metadata_obj = ChunkMetadata(
                    path=str(path.relative_to(self.repo_path)),
                    language="markdown",
                    kind="documentation",
                    symbol=current_title
                )
                
                doc = Document(
                    page_content=section_content,
                    metadata=asdict(metadata_obj)
                )
                chunks.append(doc)
                self.metadata_list.append(metadata_obj)
        
        return chunks
    
    def parse_yaml_file(self, path: Path) -> List[Document]:
        """
        Парсинг YAML файла
        
        Args:
            path: Путь к .yaml/.yml файлу
            
        Returns:
            Список Document объектов (чанки)
        """
        try:
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
        except Exception as e:
            print(f"Ошибка чтения {path}: {e}")
            return []
        
        # Для YAML файлов создаем один чанк со всем содержимым
        metadata_obj = ChunkMetadata(
            path=str(path.relative_to(self.repo_path)),
            language="yaml",
            kind="config",
            symbol=path.name
        )
        
        doc = Document(
            page_content=content,
            metadata=asdict(metadata_obj)
        )
        
        self.metadata_list.append(metadata_obj)
        return [doc]
    
    def build_index(self, skip_parsing: bool = False) -> Tuple[List[Document], FAISS]:
        """
        Построение индекса для всего репозитория
        
        Args:
            skip_parsing: Если True, пропустить парсинг и загрузить сохраненные документы
        
        Returns:
            Кортеж (список документов, FAISS векторное хранилище)
        """
        # Проверяем, есть ли уже сохраненные документы
        documents_path = self.output_dir / "documents.pkl"
        if skip_parsing and documents_path.exists():
            print(f"Загрузка сохраненных документов из {documents_path}...")
            with open(documents_path, 'rb') as f:
                self.documents = pickle.load(f)
            print(f"Загружено {len(self.documents)} документов")
            
            # Загружаем метаданные
            metadata_path_temp = self.output_dir / "metadata_list.jsonl"
            if metadata_path_temp.exists():
                self.metadata_list = []
                with open(metadata_path_temp, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            self.metadata_list.append(json.loads(line))
        else:
            print(f"Сканирование репозитория: {self.repo_path}")
            files = self.scan_files()
            print(f"Найдено файлов: {len(files)}")
            
            # Группируем по языку для статистики
            stats = {"python": 0, "cpp": 0, "yamake": 0, "cmake": 0, "protobuf": 0, "yaml": 0, "config": 0, "markdown": 0, "other": 0}
            
            # Обрабатываем файлы с прогресс-баром
            for file_path in tqdm(files, desc="Обработка файлов"):
                language = self._get_language(file_path)
                chunks = []
                
                try:
                    if language == "python":
                        chunks = self.parse_python_file(file_path)
                        stats["python"] += len(chunks)
                    elif language == "cpp":
                        chunks = self.parse_cpp_file(file_path)
                        stats["cpp"] += len(chunks)
                    elif language == "yamake":
                        chunks = self.parse_yamake_file(file_path)
                        stats["yamake"] += len(chunks)
                    elif language == "protobuf":
                        chunks = self.parse_protobuf_file(file_path)
                        stats["protobuf"] = stats.get("protobuf", 0) + len(chunks)
                    elif language == "yaml":
                        chunks = self.parse_yaml_file(file_path)
                        stats["yaml"] = stats.get("yaml", 0) + len(chunks)
                    elif language == "config":
                        chunks = self.parse_config_file(file_path)
                        stats["config"] = stats.get("config", 0) + len(chunks)
                    elif language == "markdown":
                        chunks = self.parse_markdown_file(file_path)
                        stats["markdown"] = stats.get("markdown", 0) + len(chunks)
                    else:
                        stats["other"] += 1
                        continue
                    
                    self.documents.extend(chunks)
                    
                    if chunks:
                        tqdm.write(f"Обработано {file_path.name}: {len(chunks)} чанков")
                
                except Exception as e:
                    print(f"Ошибка обработки {file_path}: {e}")
                    continue
            
            print(f"\nСтатистика индексации:")
            print(f"  Python чанков: {stats['python']}")
            print(f"  C++ чанков: {stats['cpp']}")
            print(f"  ya.make таргетов: {stats['yamake']}")
            if stats.get('protobuf', 0) > 0:
                print(f"  Protobuf чанков: {stats['protobuf']}")
            if stats.get('yaml', 0) > 0:
                print(f"  YAML файлов: {stats['yaml']}")
            if stats.get('config', 0) > 0:
                print(f"  Config файлов: {stats['config']}")
            if stats.get('markdown', 0) > 0:
                print(f"  Markdown секций: {stats['markdown']}")
            print(f"  Всего документов: {len(self.documents)}")
        
        # Если документы уже загружены, показываем статистику
        if skip_parsing:
            stats = {"python": 0, "cpp": 0, "yamake": 0, "cmake": 0, "protobuf": 0, "yaml": 0, "config": 0, "markdown": 0, "other": 0}
            for doc in self.documents:
                lang = doc.metadata.get('language', 'unknown')
                if lang == 'python':
                    stats['python'] += 1
                elif lang == 'cpp':
                    stats['cpp'] += 1
                elif lang == 'yamake':
                    stats['yamake'] += 1
                elif lang == 'protobuf':
                    stats['protobuf'] += 1
                elif lang == 'yaml':
                    stats['yaml'] += 1
                elif lang == 'config':
                    stats['config'] += 1
                elif lang == 'markdown':
                    stats['markdown'] += 1
                else:
                    stats['other'] += 1
            print(f"\nСтатистика загруженных документов:")
            print(f"  Python чанков: {stats['python']}")
            print(f"  C++ чанков: {stats['cpp']}")
            print(f"  ya.make таргетов: {stats['yamake']}")
            if stats.get('protobuf', 0) > 0:
                print(f"  Protobuf чанков: {stats['protobuf']}")
            if stats.get('yaml', 0) > 0:
                print(f"  YAML файлов: {stats['yaml']}")
            if stats.get('config', 0) > 0:
                print(f"  Config файлов: {stats['config']}")
            if stats.get('markdown', 0) > 0:
                print(f"  Markdown секций: {stats['markdown']}")
            print(f"  Всего документов: {len(self.documents)}")
        
        if not self.documents:
            print("Предупреждение: не найдено документов для индексации!")
            return [], None
        
        # Сохраняем документы для возможности продолжения
        documents_path = self.output_dir / "documents.pkl"
        print(f"\nСохранение документов в {documents_path}...")
        with open(documents_path, 'wb') as f:
            pickle.dump(self.documents, f)
        print(f"Сохранено {len(self.documents)} документов")
        
        # Сохраняем метаданные
        metadata_path_temp = self.output_dir / "metadata_list.jsonl"
        with open(metadata_path_temp, 'w', encoding='utf-8') as f:
            for metadata in self.metadata_list:
                # Проверяем, является ли metadata объектом ChunkMetadata или уже словарем
                if isinstance(metadata, dict):
                    meta_dict = metadata
                elif hasattr(metadata, '__dict__'):
                    meta_dict = asdict(metadata) if hasattr(metadata, '__class__') and hasattr(metadata.__class__, '__dataclass_fields__') else metadata.__dict__
                else:
                    meta_dict = metadata
                f.write(json.dumps(meta_dict, ensure_ascii=False) + '\n')
        
        # Создаем FAISS индекс с прогресс-баром
        print("\nСоздание векторного индекса FAISS...")
        
        # Проверяем, есть ли уже сохраненные эмбеддинги
        embeddings_path = self.output_dir / "embeddings.npy"
        if embeddings_path.exists():
            print(f"Загрузка сохраненных эмбеддингов из {embeddings_path}...")
            import numpy as np
            embeddings_list = np.load(str(embeddings_path), allow_pickle=True).tolist()
            print(f"Загружено {len(embeddings_list)} эмбеддингов")
        else:
            print(f"Генерация эмбеддингов для {len(self.documents)} документов...")
            
            # Используем батчинг для более эффективной генерации эмбеддингов
            batch_size = 32
            texts = [doc.page_content for doc in self.documents]
            metadatas = [doc.metadata for doc in self.documents]
            
            # Генерируем эмбеддинги батчами с прогресс-баром
            embeddings_list = []
            for i in tqdm(range(0, len(texts), batch_size), desc="Генерация эмбеддингов", unit="батч"):
                batch_texts = texts[i:i + batch_size]
                # Используем embed_documents для батча (быстрее чем по одному)
                batch_embeddings = self.embeddings.embed_documents(batch_texts)
                embeddings_list.extend(batch_embeddings)
            
            # Сохраняем эмбеддинги для возможности продолжения
            print(f"Сохранение эмбеддингов в {embeddings_path}...")
            import numpy as np
            np.save(str(embeddings_path), np.array(embeddings_list))
            print(f"Сохранено {len(embeddings_list)} эмбеддингов")
        
        texts = [doc.page_content for doc in self.documents]
        metadatas = [doc.metadata for doc in self.documents]
        
        # Проверяем, есть ли уже готовый индекс
        index_path = self.output_dir / "faiss_index"
        if index_path.exists() and (index_path / "index.faiss").exists():
            print(f"\nНайден существующий индекс в {index_path}")
            print("Загрузка индекса...")
            try:
                vectorstore = FAISS.load_local(str(index_path), self.embeddings, allow_dangerous_deserialization=True)
                print("Индекс успешно загружен!")
                
                # Загружаем метаданные если нужно
                metadata_path = self.output_dir / "metadata.jsonl"
                if not metadata_path.exists() and self.metadata_list:
                    print(f"Сохранение метаданных в {metadata_path}...")
                    with open(metadata_path, 'w', encoding='utf-8') as f:
                        for metadata in tqdm(self.metadata_list, desc="Сохранение метаданных", unit="зап"):
                            f.write(json.dumps(asdict(metadata) if hasattr(metadata, '__dict__') else metadata, ensure_ascii=False) + '\n')
                
                return self.documents, vectorstore
            except Exception as e:
                print(f"Ошибка загрузки индекса: {e}")
                print("Создаем новый индекс...")
        
        # Создаем индекс из готовых эмбеддингов
        print("\nСоздание FAISS индекса...")
        print("  Импорт библиотек...")
        import numpy as np
        import faiss
        # FAISS уже импортирован в начале файла, используем его
        print("  Импорт завершен")
        
        with tqdm(total=4, desc="Построение индекса", unit="шаг") as pbar:
            print("  Конвертация эмбеддингов в numpy массив...")
            print(f"    Всего эмбеддингов: {len(embeddings_list)}")
            print(f"    Размерность: {len(embeddings_list[0]) if embeddings_list else 0}")
            embeddings_array = np.array(embeddings_list).astype('float32')
            dimension = embeddings_array.shape[1]
            print(f"  Размер массива: {embeddings_array.shape}")
            pbar.update(1)
            
            # Создаем индекс FAISS
            print("  Создание FAISS индекса...")
            index = faiss.IndexFlatL2(dimension)
            pbar.update(1)
            
            print("  Добавление векторов в индекс...")
            # Добавляем векторы батчами для больших объемов
            batch_size = 10000
            if len(embeddings_array) > batch_size:
                for i in range(0, len(embeddings_array), batch_size):
                    batch = embeddings_array[i:i + batch_size]
                    index.add(batch)
                    if i % (batch_size * 10) == 0:
                        print(f"    Добавлено {min(i + batch_size, len(embeddings_array))} / {len(embeddings_array)} векторов")
            else:
                index.add(embeddings_array)
            pbar.update(1)
            
            # Создаем vectorstore обертку из готового индекса напрямую
            # Не используем from_texts, чтобы не генерировать эмбеддинги заново
            print("  Инициализация vectorstore...")
            # Используем уже импортированный FAISS из начала файла
            # Создаем через from_texts с минимальным набором, затем заменяем индекс
            vectorstore = FAISS.from_texts(
                texts=["dummy"],  # временный текст
                embedding=self.embeddings,
                metadatas=[{}]
            )
            # Заменяем индекс на наш готовый
            vectorstore.index = index
            
            # Инициализируем docstore с документами
            try:
                from langchain_core.documents import Document as LangchainDoc
            except ImportError:
                from langchain.docstore.document import Document as LangchainDoc
            
            # Используем правильный класс Docstore
            try:
                from langchain_community.docstore.in_memory import InMemoryDocstore
            except ImportError:
                try:
                    from langchain.docstore.in_memory import InMemoryDocstore
                except ImportError:
                    # Fallback: создаем простой docstore-объект с правильным интерфейсом
                    class SimpleDocstore:
                        def __init__(self):
                            self._dict = {}
                        def add(self, ids_and_docs):
                            for id, doc in ids_and_docs:
                                self._dict[id] = doc
                        def search(self, search: str):
                            return self._dict.get(search)
                    InMemoryDocstore = SimpleDocstore
            
            # Создаем docstore с прогресс-баром
            print("  Создание docstore...")
            docstore = InMemoryDocstore()
            index_to_id = {}
            
            # Подготавливаем документы для добавления
            docs_to_add = []
            for i, doc in tqdm(enumerate(self.documents), total=len(self.documents), desc="  Инициализация docstore", unit="док", leave=False):
                doc_id = str(i)
                # Используем уже существующий Document объект, если он совместим
                if isinstance(doc, LangchainDoc):
                    langchain_doc = doc
                else:
                    langchain_doc = LangchainDoc(page_content=doc.page_content, metadata=doc.metadata)
                docs_to_add.append((doc_id, langchain_doc))
                index_to_id[i] = doc_id
            
            # Добавляем все документы в docstore
            # Всегда заполняем напрямую через _dict, так как add() имеет проблемы с форматом
            if not hasattr(docstore, '_dict'):
                # Инициализируем _dict если его нет
                docstore._dict = {}
            
            # Заполняем напрямую через _dict
            for doc_id, langchain_doc in tqdm(docs_to_add, desc="  Заполнение docstore", unit="док", leave=False, total=len(docs_to_add)):
                docstore._dict[doc_id] = langchain_doc
            
            vectorstore.docstore = docstore
            vectorstore.index_to_docstore_id = index_to_id
            pbar.update(1)
        
        # Сохраняем индекс
        index_path = self.output_dir / "faiss_index"
        print(f"Сохранение индекса в {index_path}...")
        vectorstore.save_local(str(index_path))
        
        # Сохраняем метаданные в JSONL
        metadata_path = self.output_dir / "metadata.jsonl"
        print(f"Сохранение метаданных в {metadata_path}...")
        with open(metadata_path, 'w', encoding='utf-8') as f:
            for metadata in tqdm(self.metadata_list, desc="Сохранение метаданных", unit="зап"):
                # Проверяем, является ли metadata объектом ChunkMetadata или уже словарем
                if isinstance(metadata, dict):
                    meta_dict = metadata
                elif hasattr(metadata, '__dict__'):
                    meta_dict = asdict(metadata) if hasattr(metadata, '__class__') and hasattr(metadata.__class__, '__dataclass_fields__') else metadata.__dict__
                else:
                    meta_dict = metadata
                f.write(json.dumps(meta_dict, ensure_ascii=False) + '\n')
        
        # Сохраняем статистику
        stats_path = self.output_dir / "stats.json"
        if 'stats' not in locals():
            # Если stats не определен, создаем из метаданных
            stats = {"python": 0, "cpp": 0, "yamake": 0, "cmake": 0, "protobuf": 0, "yaml": 0, "config": 0, "markdown": 0, "other": 0}
            for doc in self.documents:
                lang = doc.metadata.get('language', 'unknown')
                if lang == 'python':
                    stats['python'] += 1
                elif lang == 'cpp':
                    stats['cpp'] += 1
                elif lang == 'yamake':
                    stats['yamake'] += 1
                elif lang == 'protobuf':
                    stats['protobuf'] += 1
                elif lang == 'yaml':
                    stats['yaml'] += 1
                elif lang == 'config':
                    stats['config'] += 1
                elif lang == 'markdown':
                    stats['markdown'] += 1
                else:
                    stats['other'] += 1
        stats["total_documents"] = len(self.documents)
        try:
            stats["total_files"] = len(files)
        except NameError:
            pass  # files не определен, если использовали skip_parsing
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, ensure_ascii=False)
        
        print(f"\nИндексация завершена!")
        print(f"  Индекс: {index_path}")
        print(f"  Метаданные: {metadata_path}")
        print(f"  Статистика: {stats_path}")
        
        return self.documents, vectorstore


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="RAG Index Builder для репозитория")
    parser.add_argument("--repo", type=str, default=".", 
                       help="Путь к корню репозитория (по умолчанию: текущая директория)")
    parser.add_argument("--output", type=str, default="./rag_index",
                       help="Директория для сохранения индекса (по умолчанию: ./rag_index)")
    parser.add_argument("--skip-parsing", action="store_true",
                       help="Пропустить парсинг файлов и загрузить сохраненные документы")
    parser.add_argument("--skip-embeddings", action="store_true",
                       help="Пропустить генерацию эмбеддингов и использовать сохраненные")
    parser.add_argument("--load-only", action="store_true",
                       help="Только загрузить существующий индекс без пересоздания")
    parser.add_argument("--test-search", type=str, default=None,
                       help="Выполнить тестовый поиск с указанным запросом")
    parser.add_argument("--config", type=str, default=None,
                       help="Путь к конфигурационному файлу (по умолчанию: index_config.json рядом со скриптом)")
    
    args = parser.parse_args()
    
    # Создаем индексатор
    indexer = RepoIndexer(args.repo, args.output, config_path=args.config)
    
    # Если только загрузка, просто загружаем индекс
    if args.load_only:
        index_path = Path(args.output) / "faiss_index"
        if not index_path.exists():
            print(f"Ошибка: индекс не найден в {index_path}")
            sys.exit(1)
        
        print(f"Загрузка индекса из {index_path}...")
        try:
            vectorstore = FAISS.load_local(str(index_path), indexer.embeddings, allow_dangerous_deserialization=True)
            
            # Исправляем docstore если он загрузился как словарь
            if isinstance(vectorstore.docstore, dict):
                print("  Исправление docstore (загружен как dict)...")
                try:
                    from langchain_community.docstore.in_memory import InMemoryDocstore
                except ImportError:
                    try:
                        from langchain.docstore.in_memory import InMemoryDocstore
                    except ImportError:
                        # Fallback: создаем простой docstore-объект
                        class SimpleDocstore:
                            def __init__(self):
                                self._dict = {}
                            def add(self, ids_and_docs):
                                for id, doc in ids_and_docs:
                                    self._dict[id] = doc
                            def search(self, search: str):
                                return self._dict.get(search)
                        InMemoryDocstore = SimpleDocstore
                
                # Создаем правильный docstore из словаря
                docstore_dict = vectorstore.docstore
                new_docstore = InMemoryDocstore()
                
                # Заполняем docstore напрямую через _dict (более надежно)
                if hasattr(new_docstore, '_dict'):
                    new_docstore._dict = docstore_dict.copy()
                else:
                    # Если нет _dict, заполняем напрямую через внутренний атрибут
                    # Пробуем разные варианты имен атрибутов
                    for attr_name in ['_dict', 'docstore', 'docs']:
                        if hasattr(new_docstore, attr_name):
                            setattr(new_docstore, attr_name, docstore_dict.copy())
                            break
                    else:
                        # Последний вариант - используем add, но с правильным форматом
                        # InMemoryDocstore.add ожидает список кортежей (id, doc)
                        try:
                            ids_and_docs = [(id, doc) for id, doc in docstore_dict.items()]
                            new_docstore.add(ids_and_docs)
                        except Exception as e:
                            print(f"    Предупреждение: не удалось использовать add(): {e}")
                            print("    Используем прямое присваивание...")
                            # Прямое присваивание через __dict__
                            new_docstore.__dict__['_dict'] = docstore_dict.copy()
                
                vectorstore.docstore = new_docstore
                print("  Docstore исправлен!")
            
            print("Индекс успешно загружен!")
            
            # Тест поиска если указан
            if args.test_search:
                print(f"\nТестовый поиск: '{args.test_search}'")
                print("=" * 60)
                print("Порядок результатов: по возрастанию релевантности (схожести)")
                print("Результаты отсортированы по возрастанию расстояния (score)")
                print("Чем меньше score (расстояние L2), тем выше релевантность")
                print("Результат 1 = самый релевантный (минимальное расстояние)")
                print("=" * 60)
                
                # Используем similarity_search_with_score для получения score
                # FAISS с L2 расстоянием возвращает результаты по возрастанию расстояния (меньший score = выше релевантность)
                results_with_scores = vectorstore.similarity_search_with_score(args.test_search, k=5)
                
                # Сортируем по возрастанию score (меньший = выше релевантность)
                results_with_scores = sorted(results_with_scores, key=lambda x: x[1])
                
                for i, (doc, score) in enumerate(results_with_scores, 1):
                    print(f"\nРезультат {i} (score: {score:.4f} - {'высокая' if score < 0.5 else 'средняя' if score < 1.0 else 'низкая'} релевантность):")
                    print(f"  PATH: {doc.metadata.get('path', 'N/A')}")
                    print(f"  SYMBOL: {doc.metadata.get('symbol', 'N/A')}")
                    print(f"  KIND: {doc.metadata.get('kind', 'N/A')}")
                    print(f"  LANGUAGE: {doc.metadata.get('language', 'N/A')}")
                    if doc.metadata.get('target'):
                        print(f"  TARGET: {doc.metadata['target']}")
                    print("  ---")
                    print(f"  Содержимое (первые 200 символов):")
                    print(f"  {doc.page_content[:200]}...")
            else:
                print("\nИспользуйте --test-search 'запрос' для тестирования поиска")
            
            sys.exit(0)
        except Exception as e:
            print(f"Ошибка загрузки индекса: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    # Строим индекс
    docs, vectorstore = indexer.build_index(skip_parsing=args.skip_parsing)
    
    if vectorstore and len(docs) > 0:
        # Тест поиска
        print("\n" + "="*60)
        print("Тест поиска:")
        print("="*60)
        
        test_query = args.test_search or "test_etl_pipeline dag"
        print(f"\nЗапрос: '{test_query}'")
        print("=" * 60)
        print("Порядок результатов: по возрастанию релевантности (схожести)")
        print("Результаты отсортированы по возрастанию расстояния (score)")
        print("Чем меньше score (расстояние L2), тем выше релевантность")
        print("Результат 1 = самый релевантный (минимальное расстояние)")
        print("=" * 60)
        try:
            # Используем similarity_search_with_score для получения score
            # FAISS с L2 расстоянием возвращает результаты по возрастанию расстояния (меньший score = выше релевантность)
            results_with_scores = vectorstore.similarity_search_with_score(test_query, k=5)
            
            # Сортируем по возрастанию score (меньший = выше релевантность)
            results_with_scores = sorted(results_with_scores, key=lambda x: x[1])
            
            for i, (doc, score) in enumerate(results_with_scores, 1):
                print(f"\nРезультат {i} (score: {score:.4f} - {'высокая' if score < 0.5 else 'средняя' if score < 1.0 else 'низкая'} релевантность):")
                print(f"  PATH: {doc.metadata.get('path', 'N/A')}")
                print(f"  SYMBOL: {doc.metadata.get('symbol', 'N/A')}")
                print(f"  KIND: {doc.metadata.get('kind', 'N/A')}")
                print(f"  LANGUAGE: {doc.metadata.get('language', 'N/A')}")
                if doc.metadata.get('target'):
                    print(f"  TARGET: {doc.metadata['target']}")
                print("  ---")
                print(f"  Содержимое (первые 200 символов):")
                print(f"  {doc.page_content[:200]}...")
        except Exception as e:
            print(f"Ошибка при поиске: {e}")
            import traceback
            traceback.print_exc()
