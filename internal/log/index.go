package log

import (
	"github.com/tysonmote/gommap"
	"io"
	"os"
)

var (
	offWidth uint64 = 4                   // смещение записи (offset) (4 байта, uint32)
	posWidth uint64 = 8                   // позиция записи в store файле (8 байт, uint64)
	entWidth        = offWidth + posWidth // размер одной записи в индексе (12 байт)
)

type index struct {
	file *os.File    // Файл, где хранится индекс
	mmap gommap.MMap // Memory-mapped файл для быстрого доступа
	size uint64      // Текущий размер индекса
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	// Расширение файла до максимального размера
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	// Отображение файла в памяти для работы ним как с массивом байтов
	// Позволяет работать с файлом без лишних syscall
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE, // разрешает чтение и запись.
		gommap.MAP_SHARED,                  // изменения синхронизируются с файлом на диске
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// Close закрывает индекс, гарантируя, что все данные из memory-mapped файла и буферов записаны на диск
func (i *index) Close() error {
	// гарантируется, что все изменения в mmap синхронизированы с файлом на диске
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	// гарантируется, что данные записаны в стабильное хранилище (например, на диск, а не просто в кеш ОС)
	if err := i.file.Sync(); err != nil {
		return err
	}

	// Убираются пустые байты в конце файла, оставляя только реальные данные
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

// Read читает запись индекса и возвращает:
// смещение записи (out) и позицию (pos) этой записи в файле хранилища (store).
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// in — запрашиваемый offset (если -1, читаем последнюю запись)
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write добавляет запись в индекс, записывая offset и позицию в store-файле в memory-mapped файл (mmap)
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+offWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	i.size += entWidth

	return nil
}

// Name метод возвращает путь к файлу индекса
func (i *index) Name() string {
	return i.file.Name()
}
